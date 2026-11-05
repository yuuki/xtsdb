package ingest

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
	"github.com/yuuki/xtsdb/internal/tlstest"
)

var testLayout = model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: time.Minute.Milliseconds()}

func newTestWriter(t *testing.T) (*Writer, *storagetest.Hot, *storagetest.Index) {
	t.Helper()
	hot := storagetest.NewHot(testLayout, time.Minute)
	index := storagetest.NewIndex()
	w, err := NewWriter(hot, index, WriterOptions{
		MaxSampleFuture:       10 * time.Minute,
		MaxSampleAge:          time.Duration(testLayout.ChunkRange+testLayout.Grace)*time.Millisecond + time.Minute,
		IndexCacheSize:        1000,
		IndexWriteConcurrency: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w, hot, index
}

func readAll(t *testing.T, hot *storagetest.Hot, lset labels.Labels) []model.Sample {
	t.Helper()
	id := model.ID(lset)
	got, err := hot.Read(context.Background(), []model.SeriesID{id}, time.Now().Add(-6*time.Hour).UnixMilli(), time.Now().Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	return got[id]
}

func TestWriterIndexesOncePerDay(t *testing.T) {
	w, hot, index := newTestWriter(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	lset := labels.FromStrings("__name__", "up", "job", "a")
	for i := range 3 {
		res, err := w.Write(ctx, []Series{{Labels: lset, Samples: []model.Sample{{T: now + int64(i), V: 1}}}})
		if err != nil || res.Accepted != 1 {
			t.Fatalf("write %d: %+v %v", i, res, err)
		}
	}
	// 1 series + 2 label values + 2 label names, written once.
	if index.Writes != 5 {
		t.Fatalf("index writes = %d, want 5", index.Writes)
	}
	if got := readAll(t, hot, lset); len(got) != 3 {
		t.Fatalf("stored %d samples, want 3", len(got))
	}
}

func TestWriterRejections(t *testing.T) {
	w, _, _ := newTestWriter(t)
	now := time.Now()
	res, err := w.Write(context.Background(), []Series{
		{Labels: labels.FromStrings("job", "no-name"), Samples: []model.Sample{{T: now.UnixMilli(), V: 1}}},
		{Labels: labels.FromStrings("__name__", "m"), Samples: []model.Sample{
			{T: now.UnixMilli(), V: 1},
			{T: now.Add(time.Hour).UnixMilli(), V: 1},       // too far in the future
			{T: now.Add(-48 * time.Hour).UnixMilli(), V: 1}, // window long closed
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 1 || res.Invalid != 1 || res.TooNew != 1 || res.TooOld != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	if !errors.Is(res.FirstInvalid, model.ErrNoMetricName) {
		t.Fatalf("FirstInvalid = %v", res.FirstInvalid)
	}
}

func TestWriterDoesNotStoreUnindexedSamples(t *testing.T) {
	w, hot, index := newTestWriter(t)
	index.FailWrites = errors.New("cassandra down")
	lset := labels.FromStrings("__name__", "up")
	_, err := w.Write(context.Background(), []Series{{Labels: lset, Samples: []model.Sample{{T: time.Now().UnixMilli(), V: 1}}}})
	if !errors.Is(err, ErrStorage) {
		t.Fatalf("expected ErrStorage, got %v", err)
	}
	if hot.Len() != 0 {
		t.Fatal("samples must not be stored when indexing fails")
	}
}

// Samples beyond the maximum chunk size are rejected and reported, so that
// remote write answers 400 instead of dropping them silently.
func TestWriterReportsFullChunks(t *testing.T) {
	w, hot, _ := newTestWriter(t)
	hot.MaxChunk = 2
	h := RemoteWriteHandler(w, testRWOptions, slog.New(slog.DiscardHandler))
	now := time.Now().UnixMilli()
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "busy"}},
		Samples: []prompb.Sample{{Timestamp: now, Value: 1}, {Timestamp: now + 1, Value: 2}, {Timestamp: now + 2, Value: 3}},
	}}}
	body, _ := req.Marshal()
	rec := post(t, h, "", body)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "maximum size") {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	res, err := w.Write(context.Background(), []Series{{Labels: labels.FromStrings("__name__", "busy"),
		Samples: []model.Sample{{T: now + 3, V: 4}}}})
	if err != nil || res.ChunkFull != 1 || res.Rejected() != 1 {
		t.Fatalf("result %+v %v", res, err)
	}
}

// Two label sets with the same series ID: the one recorded first keeps the
// ID, samples of the other are rejected and none of its index entries are
// written.
func TestWriterRejectsCollidingSeries(t *testing.T) {
	w, hot, index := newTestWriter(t)
	w.idOf = func(labels.Labels) model.SeriesID { return 42 } // every series collides
	ctx := context.Background()
	now := time.Now().UnixMilli()
	a := labels.FromStrings("__name__", "up", "job", "a")
	b := labels.FromStrings("__name__", "up", "job", "b")
	write := func(lset labels.Labels, ts int64) Result {
		t.Helper()
		res, err := w.Write(ctx, []Series{{Labels: lset, Samples: []model.Sample{{T: ts, V: 1}, {T: ts + 1, V: 2}}}})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if res := write(a, now); res.Accepted != 2 {
		t.Fatalf("first series: %+v", res)
	}
	if res := write(b, now+10); res.Collision != 2 || res.Accepted != 0 || res.Rejected() != 2 {
		t.Fatalf("colliding series must be rejected: %+v", res)
	}
	// The owner keeps writing (from the cache), the other one is still
	// rejected without asking the index again.
	writes := index.Writes
	if res := write(a, now+20); res.Accepted != 2 {
		t.Fatalf("owner: %+v", res)
	}
	if res := write(b, now+30); res.Collision != 2 {
		t.Fatalf("colliding series must stay rejected: %+v", res)
	}
	if index.Writes != writes {
		t.Fatalf("index written %d more times", index.Writes-writes)
	}
	// Both in one batch.
	res, err := w.Write(ctx, []Series{
		{Labels: b, Samples: []model.Sample{{T: now + 40, V: 1}}},
		{Labels: a, Samples: []model.Sample{{T: now + 41, V: 1}}},
	})
	if err != nil || res.Accepted != 1 || res.Collision != 1 {
		t.Fatalf("mixed batch: %+v %v", res, err)
	}
	got, err := hot.Read(ctx, []model.SeriesID{42}, now-1000, now+1000)
	if err != nil || len(got[42]) != 5 {
		t.Fatalf("hot tier holds %v for ID 42, want the 5 samples of the owner (%v)", got[42], err)
	}
	vals, _ := index.LabelValues(ctx, model.Day(now), "job")
	if fmt.Sprint(vals) != "[a]" {
		t.Fatalf("label values of the colliding series must not be indexed: %v", vals)
	}
	if !strings.Contains(describeRejections(Result{Collision: 3}), "collides") {
		t.Fatal("collisions must be described")
	}
}

func TestParseGraphiteLine(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cases := []struct {
		line   string
		labels string
		t      int64
		v      float64
		ok     bool
		err    bool
	}{
		{line: "servers.web01.cpu 12.5 1700000123", labels: `{__name__="servers.web01.cpu"}`, t: 1_700_000_123_000, v: 12.5, ok: true},
		{line: "cpu;host=a;dc=x 1 1700000123.5", labels: `{__name__="cpu", dc="x", host="a"}`, t: 1_700_000_123_500, v: 1, ok: true},
		{line: "cpu 3", labels: `{__name__="cpu"}`, t: now.UnixMilli(), v: 3, ok: true},
		{line: "cpu 3 -1", labels: `{__name__="cpu"}`, t: now.UnixMilli(), v: 3, ok: true},
		{line: "cpu nan 1700000000", ok: false},
		{line: "cpu", err: true},
		{line: "cpu x 1", err: true},
		{line: "cpu 1 x", err: true},
		{line: "cpu;bad 1 1", err: true},
		{line: "cpu;__name__=x 1 1", err: true},
	}
	for _, c := range cases {
		s, ok, err := ParseGraphiteLine(c.line, now)
		if (err != nil) != c.err {
			t.Errorf("%q: err = %v", c.line, err)
			continue
		}
		if err != nil || ok != c.ok {
			if ok != c.ok {
				t.Errorf("%q: ok = %v", c.line, ok)
			}
			continue
		}
		if !ok {
			continue
		}
		lset, _ := model.Canonicalize(s.Labels)
		if lset.String() != c.labels || s.Samples[0].T != c.t || s.Samples[0].V != c.v {
			t.Errorf("%q: got %s %v", c.line, lset, s.Samples)
		}
	}
}

func TestGraphiteServer(t *testing.T) {
	w, hot, _ := newTestWriter(t)
	g := NewGraphiteServer("127.0.0.1:0", 16, w, nil, slog.New(slog.DiscardHandler))
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	defer g.Stop(context.Background())

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	fmt.Fprintf(conn, "a.b.c;host=h1 1 %d\na.b.c;host=h1 2 %d\ngarbage\n", now-1, now)
	fmt.Fprintf(conn, "a.b.c;host=h2 3 %d", now) // no trailing newline
	conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h1 := readAll(t, hot, labels.FromStrings("__name__", "a.b.c", "host", "h1"))
		h2 := readAll(t, hot, labels.FromStrings("__name__", "a.b.c", "host", "h2"))
		if len(h1) == 2 && len(h2) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("graphite samples were not stored")
}

// Over TLS with client certificates, only clients with a certificate from
// the configured CA can write, and UDP is not served.
func TestGraphiteServerTLS(t *testing.T) {
	w, hot, _ := newTestWriter(t)
	f := tlstest.Generate(t, t.TempDir())
	g := NewGraphiteServer("127.0.0.1:0", 16, w, nil, slog.New(slog.DiscardHandler))
	var err error
	g.TLS, err = config.ServerTLSConfig{CertFile: f.ServerCert, KeyFile: f.ServerKey, ClientCAFile: f.CA}.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	defer g.Stop(context.Background())
	if g.UDPAddr() != nil {
		t.Fatal("UDP must be disabled with TLS")
	}
	now := time.Now().Unix()
	send := func(withCert bool, host string) error {
		conn, err := tls.Dial("tcp", g.Addr().String(), f.ClientConfig(t, withCert))
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := fmt.Fprintf(conn, "tls.metric;host=%s 1 %d\n", host, now); err != nil {
			return err
		}
		// TLS 1.3 reports a rejected client certificate on the first read.
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return nil
		}
		return err
	}
	if err := send(false, "anonymous"); err == nil {
		t.Fatal("a client without a certificate must be refused")
	}
	if err := send(true, "trusted"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(readAll(t, hot, labels.FromStrings("__name__", "tls.metric", "host", "trusted"))) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(readAll(t, hot, labels.FromStrings("__name__", "tls.metric", "host", "trusted"))) != 1 {
		t.Fatal("the sample sent over TLS was not stored")
	}
	if len(readAll(t, hot, labels.FromStrings("__name__", "tls.metric", "host", "anonymous"))) != 0 {
		t.Fatal("the sample of the refused client was stored")
	}
}

// Clients that connect without completing the TLS handshake give their
// connection slot back after the handshake timeout, so that they cannot
// lock out senders with valid certificates.
func TestGraphiteTLSHandshakeTimeout(t *testing.T) {
	defer func(d time.Duration) { graphiteHandshakeTimeout = d }(graphiteHandshakeTimeout)
	graphiteHandshakeTimeout = 200 * time.Millisecond
	w, hot, _ := newTestWriter(t)
	f := tlstest.Generate(t, t.TempDir())
	const maxConns = 2
	g := NewGraphiteServer("127.0.0.1:0", maxConns, w, nil, slog.New(slog.DiscardHandler))
	var err error
	g.TLS, err = config.ServerTLSConfig{CertFile: f.ServerCert, KeyFile: f.ServerKey, ClientCAFile: f.CA}.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	defer g.Stop(context.Background())
	for range maxConns { // idle connections that never send a ClientHello
		c, err := net.Dial("tcp", g.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	time.Sleep(graphiteHandshakeTimeout + 300*time.Millisecond)
	conn, err := tls.Dial("tcp", g.Addr().String(), f.ClientConfig(t, true))
	if err != nil {
		t.Fatalf("a trusted client must be served once idle handshakes time out: %v", err)
	}
	fmt.Fprintf(conn, "tls.slots 1 %d\n", time.Now().Unix())
	conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(readAll(t, hot, labels.FromStrings("__name__", "tls.slots"))) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(readAll(t, hot, labels.FromStrings("__name__", "tls.slots"))) != 1 {
		t.Fatal("the trusted client's sample was not stored")
	}
}

var testRWOptions = RemoteWriteOptions{
	MaxBodySize:         1 << 20,
	MaxDecompressedSize: 4 << 20,
	MaxInflight:         4,
	Limits:              Limits{MaxSeries: 1000, MaxSamples: 10_000},
}

func post(t *testing.T, h http.Handler, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(snappy.Encode(nil, body)))
	req.Header.Set("Content-Encoding", "snappy")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRemoteWriteV1(t *testing.T) {
	w, hot, _ := newTestWriter(t)
	h := RemoteWriteHandler(w, testRWOptions, slog.New(slog.DiscardHandler))
	now := time.Now().UnixMilli()
	req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "job", Value: "x"}, {Name: "__name__", Value: "up"}},
		Samples: []prompb.Sample{{Timestamp: now, Value: 1}, {Timestamp: now + 1000, Value: 0}},
	}}}
	body, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	rec := post(t, h, "application/x-protobuf", body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := readAll(t, hot, labels.FromStrings("__name__", "up", "job", "x")); len(got) != 2 {
		t.Fatalf("stored %v", got)
	}

	// Partial rejection is reported with 400 so that the sender does not retry.
	req.Timeseries[0].Samples = []prompb.Sample{{Timestamp: now - 48*3600*1000, Value: 1}}
	body, _ = req.Marshal()
	if rec := post(t, h, "", body); rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if rec := post(t, h, "application/json", body); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status %d, want 415", rec.Code)
	}
}

func TestRemoteWriteV2(t *testing.T) {
	w, hot, index := newTestWriter(t)
	h := RemoteWriteHandler(w, testRWOptions, slog.New(slog.DiscardHandler))
	st := writev2.NewSymbolTable()
	lset := labels.FromStrings("__name__", "http_requests_total", "code", "200")
	now := time.Now().UnixMilli()
	req := &writev2.Request{Timeseries: []writev2.TimeSeries{{
		LabelsRefs: st.SymbolizeLabels(lset, nil),
		Samples:    []writev2.Sample{{Timestamp: now, Value: 10}},
	}}}
	req.Symbols = st.Symbols()
	body, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	rec := post(t, h, "application/x-protobuf;proto=io.prometheus.write.v2.Request", body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get(headerSamplesWritten); got != "1" {
		t.Fatalf("samples written header = %q", got)
	}
	if got := readAll(t, hot, lset); len(got) != 1 || got[0].V != 10 {
		t.Fatalf("stored %v", got)
	}

	index.FailWrites = errors.New("down")
	lset2 := labels.FromStrings("__name__", "other")
	req = &writev2.Request{Timeseries: []writev2.TimeSeries{{
		LabelsRefs: st.SymbolizeLabels(lset2, nil),
		Samples:    []writev2.Sample{{Timestamp: now, Value: 1}},
	}}, Symbols: st.Symbols()}
	body, _ = req.Marshal()
	if rec := post(t, h, "application/x-protobuf;proto=io.prometheus.write.v2.Request", body); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 so that the sender retries", rec.Code)
	}
}

func TestWriterRejectsExtremeTimestampsBeforeIndexing(t *testing.T) {
	w, hot, index := newTestWriter(t)
	now := time.Now().UnixMilli()
	res, err := w.Write(context.Background(), []Series{
		{Labels: labels.FromStrings("__name__", "a"), Samples: []model.Sample{{T: math.MinInt64, V: 1}, {T: now, V: 1}}},
		{Labels: labels.FromStrings("__name__", "b"), Samples: []model.Sample{{T: 1000, V: 1}, {T: now, V: 1}}},
		{Labels: labels.FromStrings("__name__", "c"), Samples: []model.Sample{{T: math.MaxInt64, V: 1}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 2 || res.TooOld != 2 || res.TooNew != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	// Two series on one (or two, around midnight) days: never thousands.
	if index.Writes > 2*5 {
		t.Fatalf("too-old samples caused %d index writes", index.Writes)
	}
	if hot.Len() != 2 {
		t.Fatalf("hot chunks = %d", hot.Len())
	}
}

func TestRemoteWriteLimits(t *testing.T) {
	w, _, _ := newTestWriter(t)
	h := RemoteWriteHandler(w, testRWOptions, slog.New(slog.DiscardHandler))

	// Decompresses beyond max_decompressed_size.
	if rec := post(t, h, "", make([]byte, 5<<20)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized payload: status %d", rec.Code)
	}
	// Many empty time series: tiny on the wire, huge once decoded.
	bomb := bytes.Repeat([]byte{0x0a, 0x00}, 2000)
	if rec := post(t, h, "", bomb); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("series bomb: status %d: %s", rec.Code, rec.Body)
	}
	// Many empty samples inside one series.
	inner := bytes.Repeat([]byte{0x12, 0x00}, 20_000)
	one := append([]byte{0x0a}, binary.AppendUvarint(nil, uint64(len(inner)))...)
	if rec := post(t, h, "", append(one, inner...)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("sample bomb: status %d: %s", rec.Code, rec.Body)
	}
	// The S2 "repeat offset" extension gives near-unbounded compression
	// ratios; the strict Snappy decoder rejects it before anything is
	// allocated.
	s2bomb := s2.EncodeBest(nil, bytes.Repeat([]byte{0x0a, 0x00}, 1<<20))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(s2bomb))
	req.Header.Set("Content-Encoding", "snappy")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "snappy") {
		t.Fatalf("s2 bomb (%d bytes): status %d: %s", len(s2bomb), rec.Code, rec.Body)
	}
}

func testHistogram(i int) *histogram.Histogram {
	return &histogram.Histogram{
		Schema:          0,
		Count:           uint64(3 + i),
		Sum:             float64(i),
		ZeroThreshold:   0.001,
		ZeroCount:       1,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []int64{1, int64(i)}, // deltas: 1, 1+i
	}
}

// Native histograms are stored next to float samples, in both protocol
// versions; invalid ones are rejected.
func TestRemoteWriteHistograms(t *testing.T) {
	w, hot, _ := newTestWriter(t)
	h := RemoteWriteHandler(w, testRWOptions, slog.New(slog.DiscardHandler))
	now := time.Now().UnixMilli()

	v1 := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{Labels: []prompb.Label{{Name: "__name__", Value: "f"}}, Samples: []prompb.Sample{{Timestamp: now, Value: 1}}},
		{Labels: []prompb.Label{{Name: "__name__", Value: "h1"}}, Histograms: []prompb.Histogram{
			prompb.FromIntHistogram(now, testHistogram(1)),
			prompb.FromFloatHistogram(now+1000, testHistogram(2).ToFloat(nil)),
		}},
	}}
	body, _ := v1.Marshal()
	if rec := post(t, h, "", body); rec.Code != http.StatusNoContent {
		t.Fatalf("v1: status %d: %s", rec.Code, rec.Body)
	}
	got := readAll(t, hot, labels.FromStrings("__name__", "h1"))
	if len(got) != 2 || got[0].H == nil || !got[0].H.Equals(testHistogram(1).ToFloat(nil)) || got[1].H.Count != 5 {
		t.Fatalf("v1 histograms stored as %v", got)
	}
	if got := readAll(t, hot, labels.FromStrings("__name__", "f")); len(got) != 1 || got[0].H != nil {
		t.Fatalf("float samples stored as %v", got)
	}

	st := writev2.NewSymbolTable()
	custom := &histogram.FloatHistogram{
		Schema: histogram.CustomBucketsSchema, Count: 3, Sum: 2,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{1, 2},
		CustomValues: []float64{0.5, 1},
	}
	v2 := &writev2.Request{Timeseries: []writev2.TimeSeries{{
		LabelsRefs: st.SymbolizeLabels(labels.FromStrings("__name__", "h2"), nil),
		Samples:    []writev2.Sample{{Timestamp: now, Value: 7}},
		Histograms: []writev2.Histogram{
			writev2.FromIntHistogram(0, now+1000, testHistogram(3)),
			writev2.FromFloatHistogram(0, now+2000, custom),
		},
	}}, Symbols: st.Symbols()}
	body, _ = v2.Marshal()
	rec := post(t, h, "application/x-protobuf;proto=io.prometheus.write.v2.Request", body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("v2: status %d: %s", rec.Code, rec.Body)
	}
	if s, hs := rec.Header().Get(headerSamplesWritten), rec.Header().Get(headerHistogramsWritten); s != "1" || hs != "2" {
		t.Fatalf("written headers: samples %q, histograms %q", s, hs)
	}
	got = readAll(t, hot, labels.FromStrings("__name__", "h2"))
	if len(got) != 3 || got[0].V != 7 || got[2].H == nil || !got[2].H.Equals(custom) {
		t.Fatalf("v2 samples stored as %v", got)
	}

	// A histogram whose count does not match its buckets is rejected.
	bad := testHistogram(4)
	bad.Count = 1
	v1 = &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{Labels: []prompb.Label{{Name: "__name__", Value: "bad"}}, Histograms: []prompb.Histogram{prompb.FromIntHistogram(now, bad)}},
	}}
	body, _ = v1.Marshal()
	if rec := post(t, h, "", body); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "histogram") {
		t.Fatalf("invalid histogram: status %d: %s", rec.Code, rec.Body)
	}
}

func TestParseGraphiteRejectsHugeTimestamps(t *testing.T) {
	for _, ts := range []string{"1e300", "-1e300", "Inf", "1e12"} {
		if _, _, err := ParseGraphiteLine("a 1 "+ts, time.Now()); err == nil {
			t.Errorf("timestamp %s must be rejected", ts)
		}
	}
}

func startGraphite(t *testing.T, maxConns int) (*GraphiteServer, *storagetest.Hot) {
	t.Helper()
	w, hot, _ := newTestWriter(t)
	g := NewGraphiteServer("127.0.0.1:0", maxConns, w, nil, slog.New(slog.DiscardHandler))
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(context.Background()) })
	return g, hot
}

func countStored(t *testing.T, hot *storagetest.Hot, name string) int {
	return len(readAll(t, hot, labels.FromStrings("__name__", name)))
}

func TestGraphiteFlushesWhileDataKeepsFlowing(t *testing.T) {
	g, hot := startGraphite(t, 16)
	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	base := time.Now().Unix() - 100
	// A line every 200ms: the connection never goes quiet for a full second.
	for i := range 12 {
		fmt.Fprintf(conn, "steady %d %d\n", i, base+int64(i))
		time.Sleep(200 * time.Millisecond)
	}
	if n := countStored(t, hot, "steady"); n < 6 {
		t.Fatalf("only %d of 12 lines stored while the connection is open", n)
	}
}

func TestGraphiteConnectionLimit(t *testing.T) {
	g, _ := startGraphite(t, 2)
	var open []net.Conn
	for range 2 {
		c, err := net.Dial("tcp", g.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		open = append(open, c)
	}
	time.Sleep(100 * time.Millisecond)
	extra, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := extra.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("connection beyond the limit must be closed, got %v", err)
	}
	_ = open
}

func TestGraphiteUDP(t *testing.T) {
	g, hot := startGraphite(t, 16)
	conn, err := net.Dial("udp", g.UDPAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ts := time.Now().Unix()
	const n = 2000
	for i := range n {
		fmt.Fprintf(conn, "udp.metric;i=%d %d %d\n", i, i, ts)
	}
	stored := func() int {
		c := 0
		for i := range n {
			c += len(readAll(t, hot, labels.FromStrings("__name__", "udp.metric", "i", strconv.Itoa(i))))
		}
		return c
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && stored() < n {
		time.Sleep(50 * time.Millisecond)
	}
	if c := stored(); c < n*95/100 { // UDP may drop a few in the kernel
		t.Fatalf("stored %d of %d UDP lines", c, n)
	}
}

func TestGraphiteStopIsPromptAndFlushes(t *testing.T) {
	g, hot := startGraphite(t, 16)
	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "pending 1 %d\n", time.Now().Unix())
	time.Sleep(50 * time.Millisecond) // the line is read but not yet flushed
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	g.Stop(ctx) // the client keeps its connection open
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop took %v with an idle client connected", d)
	}
	if countStored(t, hot, "pending") != 1 {
		t.Fatal("buffered line was not written on shutdown")
	}
}

// A histogram with millions of buckets is a few bytes per bucket on the
// wire; it is refused before it is decoded, and a large one that gets
// through is rejected instead of creating a chunk that cannot be flushed.
func TestRemoteWriteBoundsHistogramBuckets(t *testing.T) {
	w, hot, _ := newTestWriter(t)
	h := RemoteWriteHandler(w, testRWOptions, slog.New(slog.DiscardHandler))
	now := time.Now().UnixMilli()
	big := func(buckets int) *histogram.Histogram {
		return &histogram.Histogram{Schema: 0, PositiveSpans: []histogram.Span{{Length: uint32(buckets)}},
			PositiveBuckets: make([]int64, buckets)}
	}
	post1 := func(hist *histogram.Histogram) *httptest.ResponseRecorder {
		req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{{Name: "__name__", Value: "big"}}, Histograms: []prompb.Histogram{prompb.FromIntHistogram(now, hist)},
		}}}
		body, _ := req.Marshal()
		return post(t, h, "", body)
	}
	// Beyond the element limit of the request: refused on the wire.
	if rec := post1(big(testRWOptions.Limits.MaxSamples + 1)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	// Within the request limits but beyond MaxHistogramBuckets: invalid.
	res, err := w.Write(context.Background(), []Series{{Labels: labels.FromStrings("__name__", "big"),
		Samples: []model.Sample{{T: now, H: big(model.MaxHistogramBuckets + 1).ToFloat(nil)}}}})
	if err != nil || res.Invalid != 1 || !strings.Contains(fmt.Sprint(res.FirstInvalid), "buckets") {
		t.Fatalf("result %+v %v", res, err)
	}
	if got := readAll(t, hot, labels.FromStrings("__name__", "big")); len(got) != 0 {
		t.Fatalf("the oversized histogram was stored: %d samples", len(got))
	}
	if rec := post1(big(100)); rec.Code != http.StatusNoContent {
		t.Fatalf("a normal histogram must be accepted: %d %s", rec.Code, rec.Body)
	}
}
