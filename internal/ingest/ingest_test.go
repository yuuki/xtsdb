package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

var testLayout = model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: time.Minute.Milliseconds()}

func newTestWriter(t *testing.T) (*Writer, *storagetest.Hot, *storagetest.Index) {
	t.Helper()
	hot := storagetest.NewHot(testLayout, time.Minute)
	index := storagetest.NewIndex()
	w, err := NewWriter(hot, index, WriterOptions{
		MaxSampleFuture:       10 * time.Minute,
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
	got, err := hot.Read(context.Background(), []model.SeriesID{id}, 0, time.Now().Add(time.Hour).UnixMilli())
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
	g := NewGraphiteServer("127.0.0.1:0", w, nil, slog.New(slog.DiscardHandler))
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
	h := RemoteWriteHandler(w, 1<<20, slog.New(slog.DiscardHandler))
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
	h := RemoteWriteHandler(w, 1<<20, slog.New(slog.DiscardHandler))
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
