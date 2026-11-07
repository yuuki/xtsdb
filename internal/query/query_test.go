package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/flush"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

var (
	layout = model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: time.Minute.Milliseconds()}
	now    = time.Unix(1_700_000_000, 0)
)

type fixture struct {
	hot   *storagetest.Hot
	cold  *storagetest.Cold
	index *storagetest.Index
	q     *Queryable
	srv   *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		hot:   storagetest.NewHot(layout, time.Minute),
		cold:  storagetest.NewCold(layout.ChunkRange),
		index: storagetest.NewIndex(),
	}
	f.hot.Now = func() time.Time { return now }
	var err error
	f.q, err = NewQueryable(f.hot, f.cold, f.index, Options{
		HotLookback:       6 * time.Hour,
		MaxSeriesPerQuery: 100,
		SeriesCacheSize:   100,
		ReadConcurrency:   4,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.q.now = func() time.Time { return now }
	api := NewAPI(f.q, APIOptions{LookbackDelta: 5 * time.Minute, Timeout: time.Minute, MaxSamples: 1_000_000})
	api.now = func() time.Time { return now }
	mux := http.NewServeMux()
	api.Register(mux, func(h http.Handler) http.Handler { return h })
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// add indexes a series and stores samples in the hot tier.
func (f *fixture) add(t *testing.T, lset labels.Labels, samples ...model.Sample) model.SeriesID {
	t.Helper()
	id := f.indexSeries(t, lset, samples)
	if _, err := f.hot.Append(context.Background(), []storage.SeriesSamples{{ID: id, Samples: samples}}); err != nil {
		t.Fatal(err)
	}
	return id
}

// indexSeries indexes a series for the days of samples.
func (f *fixture) indexSeries(t *testing.T, lset labels.Labels, samples []model.Sample) model.SeriesID {
	t.Helper()
	ctx := context.Background()
	id := model.ID(lset)
	for _, day := range model.Days(samples[0].T, samples[len(samples)-1].T) {
		if err := f.index.WriteSeries(ctx, day, id, lset); err != nil {
			t.Fatal(err)
		}
		lset.Range(func(l labels.Label) {
			_ = f.index.WriteLabelValue(ctx, day, l.Name, l.Value)
			_ = f.index.WriteLabelName(ctx, day, l.Name)
		})
	}
	return id
}

// persist writes samples directly to the cold tier.
func (f *fixture) persist(t *testing.T, id model.SeriesID, samples ...model.Sample) {
	t.Helper()
	enc, data, err := chunk.Encode(samples)
	if err != nil {
		t.Fatal(err)
	}
	start := layout.WindowStart(id, samples[0].T)
	if err := f.cold.WriteChunk(context.Background(), storage.ColdChunk{
		Series: id, Start: start, Fragment: 1, MaxT: samples[len(samples)-1].T,
		Count: len(samples), Encoding: enc, Data: data,
	}); err != nil {
		t.Fatal(err)
	}
}

func ms(d time.Duration) int64 { return now.Add(d).UnixMilli() }

func (f *fixture) get(t *testing.T, path string, params url.Values) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(f.srv.URL + path + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func seedSeries(t *testing.T, f *fixture) {
	for _, job := range []string{"api", "db", "web"} {
		for _, inst := range []string{"a", "b"} {
			lset := labels.FromStrings("__name__", "up", "job", job, "instance", inst)
			f.add(t, lset, model.Sample{T: ms(-2 * time.Minute), V: 1}, model.Sample{T: ms(-time.Minute), V: 1})
		}
	}
}

func TestSeriesResolution(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f)
	ctx := context.Background()
	cases := []struct {
		matchers []*labels.Matcher
		want     int
	}{
		{[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}, 6},
		{[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "api")}, 2},
		{[]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "api|db")}, 4},
		{[]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "w.*")}, 2},
		{[]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
			labels.MustNewMatcher(labels.MatchNotEqual, "job", "api"),
			labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "b"),
		}, 2},
		{[]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
			labels.MustNewMatcher(labels.MatchEqual, "job", "none"),
		}, 0},
	}
	for i, c := range cases {
		refs, _, err := f.q.Series(ctx, ms(-time.Hour), ms(0), 0, c.matchers...)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if len(refs) != c.want {
			t.Errorf("case %d: got %d series, want %d", i, len(refs), c.want)
		}
	}
	_, _, err := f.q.Series(ctx, ms(-time.Hour), ms(0), 0, labels.MustNewMatcher(labels.MatchNotEqual, "job", "x"))
	if err != ErrNoPositiveMatcher {
		t.Fatalf("expected ErrNoPositiveMatcher, got %v", err)
	}
}

func TestSamplesMergeTiers(t *testing.T) {
	f := newFixture(t)
	lset := labels.FromStrings("__name__", "temp")
	id := f.add(t, lset, model.Sample{T: ms(-time.Minute), V: 2}, model.Sample{T: ms(-30 * time.Second), V: 3})
	// Older data that was already flushed, plus an overlap the hot tier wins.
	f.persist(t, id, model.Sample{T: ms(-2 * time.Hour), V: 0}, model.Sample{T: ms(-time.Minute), V: -1})

	got, err := f.q.Samples(context.Background(), []model.SeriesID{id}, ms(-3*time.Hour), ms(0))
	if err != nil {
		t.Fatal(err)
	}
	want := []model.Sample{{T: ms(-2 * time.Hour), V: 0}, {T: ms(-time.Minute), V: 2}, {T: ms(-30 * time.Second), V: 3}}
	if fmt.Sprint(got[id]) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got[id], want)
	}
}

func TestInstantQuery(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f)
	code, body := f.get(t, "/api/v1/query", url.Values{"query": {`sum by (job) (up)`}, "time": {fmt.Sprint(now.Unix())}})
	if code != http.StatusOK || body["status"] != "success" {
		t.Fatalf("status %d: %v", code, body)
	}
	data := body["data"].(map[string]any)
	if data["resultType"] != "vector" {
		t.Fatalf("resultType = %v", data["resultType"])
	}
	result := data["result"].([]any)
	if len(result) != 3 {
		t.Fatalf("got %d results, want 3", len(result))
	}
	first := result[0].(map[string]any)
	if v := first["value"].([]any)[1]; v != "2" {
		t.Fatalf("sum = %v, want 2", v)
	}
}

func TestRangeQuery(t *testing.T) {
	f := newFixture(t)
	lset := labels.FromStrings("__name__", "requests_total", "job", "api")
	var samples []model.Sample
	for i := range 10 {
		samples = append(samples, model.Sample{T: ms(time.Duration(i-10) * time.Minute), V: float64(i * 60)})
	}
	f.add(t, lset, samples...)
	code, body := f.get(t, "/api/v1/query_range", url.Values{
		"query": {`rate(requests_total[5m])`},
		"start": {fmt.Sprint(now.Add(-5 * time.Minute).Unix())},
		"end":   {fmt.Sprint(now.Unix())},
		"step":  {"60"},
	})
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	data := body["data"].(map[string]any)
	result := data["result"].([]any)
	if data["resultType"] != "matrix" || len(result) != 1 {
		t.Fatalf("unexpected result %v", data)
	}
	values := result[0].(map[string]any)["values"].([]any)
	if len(values) < 5 {
		t.Fatalf("got %d points", len(values))
	}
	for _, p := range values {
		v, _ := strconv.ParseFloat(p.([]any)[1].(string), 64)
		if math.Abs(v-1) > 1e-9 {
			t.Fatalf("rate = %v, want 1", v)
		}
	}
}

func TestUTF8MetricNames(t *testing.T) {
	f := newFixture(t)
	f.add(t, labels.FromStrings("__name__", "servers.web01.cpu"), model.Sample{T: ms(-time.Minute), V: 42})
	code, body := f.get(t, "/api/v1/query", url.Values{"query": {`{"servers.web01.cpu"}`}, "time": {fmt.Sprint(now.Unix())}})
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	if n := len(body["data"].(map[string]any)["result"].([]any)); n != 1 {
		t.Fatalf("got %d results", n)
	}
}

func TestMetadataEndpoints(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f)
	code, body := f.get(t, "/api/v1/series", url.Values{"match[]": {`up{job="api"}`}})
	if code != http.StatusOK || len(body["data"].([]any)) != 2 {
		t.Fatalf("series: %d %v", code, body)
	}
	code, body = f.get(t, "/api/v1/labels", nil)
	if code != http.StatusOK || fmt.Sprint(body["data"]) != "[__name__ instance job]" {
		t.Fatalf("labels: %d %v", code, body)
	}
	code, body = f.get(t, "/api/v1/label/job/values", nil)
	if code != http.StatusOK || fmt.Sprint(body["data"]) != "[api db web]" {
		t.Fatalf("label values: %d %v", code, body)
	}
	code, body = f.get(t, "/api/v1/label/job/values", url.Values{"match[]": {`up{instance="a",job=~"a.*|w.*"}`}})
	if code != http.StatusOK || fmt.Sprint(body["data"]) != "[api web]" {
		t.Fatalf("label values with match[]: %d %v", code, body)
	}
	code, body = f.get(t, "/api/v1/status/buildinfo", nil)
	if code != http.StatusOK || body["status"] != "success" {
		t.Fatalf("buildinfo: %d %v", code, body)
	}
}

func TestAPIErrors(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f)
	f.q.opts.MaxSeriesPerQuery = 3
	cases := []struct {
		path   string
		params url.Values
		code   int
		typ    string
	}{
		{"/api/v1/query", url.Values{"query": {"sum("}}, 400, "bad_data"},
		{"/api/v1/query", url.Values{"query": {"up"}, "time": {"yesterday"}}, 400, "bad_data"},
		{"/api/v1/query", url.Values{"query": {`{job!="x"}`}}, 400, "bad_data"},
		{"/api/v1/query", url.Values{"query": {"up"}}, 422, "execution"},
		{"/api/v1/query_range", url.Values{"query": {"up"}, "start": {"10"}, "end": {"5"}, "step": {"1"}}, 400, "bad_data"},
		{"/api/v1/query_range", url.Values{"query": {"up"}, "start": {"0"}, "end": {"100000"}, "step": {"1"}}, 400, "bad_data"},
		{"/api/v1/series", nil, 400, "bad_data"},
	}
	for _, c := range cases {
		code, body := f.get(t, c.path, c.params)
		if code != c.code || body["errorType"] != c.typ {
			t.Errorf("%s %v: got %d %v, want %d %s", c.path, c.params, code, body, c.code, c.typ)
		}
	}
}

func TestParseTimeAndDuration(t *testing.T) {
	for _, s := range []string{"1700000000", "1700000000.123", "2023-11-14T22:13:20Z"} {
		if _, err := parseTime(s); err != nil {
			t.Errorf("parseTime(%q): %v", s, err)
		}
	}
	if tm, _ := parseTime("1700000000.5"); tm.UnixMilli() != 1_700_000_000_500 {
		t.Errorf("fractional seconds lost: %v", tm)
	}
	for _, s := range []string{"", "abc", "NaN"} {
		if _, err := parseTime(s); err == nil {
			t.Errorf("parseTime(%q) should fail", s)
		}
	}
	// Huge values and the min/max sentinels sent by Prometheus clients are
	// accepted and clamped.
	for _, s := range []string{"1e12", "1e300", "-1e300", minTimeFormatted, maxTimeFormatted} {
		if _, err := parseTime(s); err != nil {
			t.Errorf("parseTime(%q): %v", s, err)
		}
	}
	for s, want := range map[string]time.Duration{"15": 15 * time.Second, "1.5": 1500 * time.Millisecond, "5m": 5 * time.Minute, "1d": 24 * time.Hour} {
		if got, err := parseDuration(s); err != nil || got != want {
			t.Errorf("parseDuration(%q) = %v, %v", s, got, err)
		}
	}
	if _, err := parseDuration("x"); err == nil || !strings.Contains(err.Error(), "cannot parse") {
		t.Errorf("parseDuration should fail: %v", err)
	}
	// The largest Duration in seconds rounds to 2^63 as a float64, which
	// used to wrap around to the most negative Duration.
	if d, err := parseDuration("9223372036.854775807"); err == nil {
		t.Errorf("parseDuration of an overflowing value = %v, want an error", d)
	}
}

// handoffCold simulates a flusher that persists and deletes a chunk at the
// moment the cold tier is read, after the cold read took its snapshot.
type handoffCold struct {
	*storagetest.Cold
	hotRead *bool
	order   []string
	handoff func()
}

func (h *handoffCold) Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	if !*h.hotRead {
		h.order = append(h.order, "cold-before-hot")
	}
	res, err := h.Cold.Read(ctx, ids, mint, maxt)
	if h.handoff != nil {
		h.handoff()
		h.handoff = nil
	}
	return res, err
}

type recordingHot struct {
	*storagetest.Hot
	read *bool
}

func (r *recordingHot) Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	*r.read = true
	return r.Hot.Read(ctx, ids, mint, maxt)
}

func TestNoGapDuringHandoff(t *testing.T) {
	f := newFixture(t)
	lset := labels.FromStrings("__name__", "handoff")
	id := f.add(t, lset, model.Sample{T: ms(-time.Minute), V: 7})

	var hotRead bool
	cold := &handoffCold{Cold: f.cold, hotRead: &hotRead}
	cold.handoff = func() {
		// Close the window, persist it and delete it from the hot tier.
		f.hot.Now = func() time.Time { return now.Add(3 * time.Hour) }
		flush.New(f.hot, f.cold, flush.Options{BatchSize: 10, Concurrency: 1, Lease: time.Minute}).FlushOnce(context.Background())
		if f.hot.Len() != 0 || f.cold.Len() != 1 {
			t.Fatalf("hand-off did not happen: hot=%d cold=%d", f.hot.Len(), f.cold.Len())
		}
	}
	q, err := NewQueryable(&recordingHot{Hot: f.hot, read: &hotRead}, cold, f.index, f.q.opts)
	if err != nil {
		t.Fatal(err)
	}
	q.now = f.q.now
	got, err := q.Samples(context.Background(), []model.SeriesID{id}, ms(-time.Hour), ms(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(cold.order) > 0 {
		t.Fatal("the cold tier must be read after the hot tier")
	}
	if len(got[id]) != 1 {
		t.Fatalf("sample lost during hand-off: %v", got[id])
	}
}

// countingIndex counts index reads; it is called concurrently.
type countingIndex struct {
	*storagetest.Index
	postings, labelNames atomic.Int64
}

func (c *countingIndex) Postings(ctx context.Context, day int32, name, value string) ([]model.SeriesID, error) {
	c.postings.Add(1)
	return c.Index.Postings(ctx, day, name, value)
}

func (c *countingIndex) LabelNames(ctx context.Context, day int32) ([]string, error) {
	c.labelNames.Add(1)
	return c.Index.LabelNames(ctx, day)
}

// countingCold counts the series whose samples were loaded.
type countingCold struct {
	*storagetest.Cold
	loaded int
}

func (c *countingCold) Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	c.loaded += len(ids)
	return c.Cold.Read(ctx, ids, mint, maxt)
}

func TestSamplesAreLoadedLazilyAndBounded(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f) // 6 series with 2 samples each
	cold := &countingCold{Cold: f.cold}
	q, err := NewQueryable(f.hot, cold, f.index, Options{
		HotLookback: 6 * time.Hour, MaxSeriesPerQuery: 100, SeriesCacheSize: 100, ReadConcurrency: 4, LoadBatchSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	q.now = f.q.now
	api := NewAPI(q, APIOptions{LookbackDelta: 5 * time.Minute, Timeout: time.Minute, MaxSamples: 3})
	api.now = f.q.now
	mux := http.NewServeMux()
	api.Register(mux, func(h http.Handler) http.Handler { return h })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/query?" + url.Values{"query": {"up[1h]"}, "time": {fmt.Sprint(now.Unix())}}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status %d, want 422 (too many samples)", resp.StatusCode)
	}
	if cold.loaded >= 6 {
		t.Fatalf("all %d series were loaded although max_samples stopped the query early", cold.loaded)
	}
}

func TestPostingsFanoutIsBounded(t *testing.T) {
	f := newFixture(t)
	idx := &countingIndex{Index: f.index}
	for i := range 500 {
		lset := labels.FromStrings("__name__", "other", "instance", fmt.Sprintf("i%d", i))
		f.add(t, lset, model.Sample{T: ms(-time.Minute), V: 1})
	}
	f.add(t, labels.FromStrings("__name__", "rare", "instance", "x"), model.Sample{T: ms(-time.Minute), V: 1})
	q, _ := NewQueryable(f.hot, f.cold, idx, f.q.opts)
	q.now = f.q.now
	for _, m := range []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+"),
		labels.MustNewMatcher(labels.MatchNotEqual, "instance", ""),
	} {
		idx.postings.Store(0)
		refs, _, err := q.Series(context.Background(), ms(-time.Hour), ms(0), 0,
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "rare"), m)
		if err != nil || len(refs) != 1 {
			t.Fatalf("%s: %v %v", m, refs, err)
		}
		if n := idx.postings.Load(); n > 2 {
			t.Fatalf("%s: %d postings reads for one candidate series", m, n)
		}
	}
}

func TestQueryRangeIsBoundedWithoutRetention(t *testing.T) {
	f := newFixture(t)
	idx := &countingIndex{Index: f.index}
	q, _ := NewQueryable(f.hot, f.cold, idx, Options{HotLookback: time.Hour, SeriesCacheSize: 10, ReadConcurrency: 4})
	q.now = f.q.now
	if _, _, err := q.LabelNames(context.Background(), math.MinInt64/2, ms(0), 0); err != nil {
		t.Fatal(err)
	}
	if n := idx.labelNames.Load(); n > 367 {
		t.Fatalf("label names read %d days", n)
	}
}

func TestEmptyResultsAreArrays(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"/api/v1/labels", "/api/v1/label/job/values", "/api/v1/series?match[]=nothing"} {
		resp, err := http.Get(f.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(b), `"data":[]`) {
			t.Errorf("%s: %s", path, b)
		}
	}
}

func TestSeriesLimitTruncates(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f)
	f.q.opts.MaxSeriesPerQuery = 3
	code, body := f.get(t, "/api/v1/series", url.Values{"match[]": {"up"}, "limit": {"2"}})
	if code != http.StatusOK || len(body["data"].([]any)) != 2 || body["warnings"] == nil {
		t.Fatalf("status %d: %v", code, body)
	}
	code, body = f.get(t, "/api/v1/label/job/values", url.Values{"limit": {"1"}})
	if code != http.StatusOK || len(body["data"].([]any)) != 1 || body["warnings"] == nil {
		t.Fatalf("label values: status %d: %v", code, body)
	}
}

// failingCold simulates a Cassandra outage.
type failingCold struct{ *storagetest.Cold }

func (failingCold) Read(context.Context, []model.SeriesID, int64, int64) (map[model.SeriesID][]model.Sample, error) {
	return nil, errors.New("connection refused")
}

func TestStorageOutageIs503(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f)
	q, _ := NewQueryable(f.hot, failingCold{f.cold}, f.index, f.q.opts)
	q.now = f.q.now
	api := NewAPI(q, APIOptions{LookbackDelta: 5 * time.Minute, Timeout: time.Minute, MaxSamples: 1000})
	api.now = f.q.now
	mux := http.NewServeMux()
	api.Register(mux, func(h http.Handler) http.Handler { return h })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/query?query=up&time=" + fmt.Sprint(now.Unix()))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), `"errorType":"unavailable"`) {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
}

func TestNonUTF8MatcherMatchesNothing(t *testing.T) {
	f := newFixture(t)
	seedSeries(t, f)
	refs, _, err := f.q.Series(context.Background(), ms(-time.Hour), ms(0), 0,
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "\xff"))
	if err != nil || len(refs) != 0 {
		t.Fatalf("got %v, %v", refs, err)
	}
}

func TestHotLookbackFollowsUnflushedChunks(t *testing.T) {
	f := newFixture(t)
	lset := labels.FromStrings("__name__", "lagging")
	// The sample is 8h old and was never flushed (flushers are behind).
	f.hot.Now = func() time.Time { return now.Add(-8 * time.Hour) }
	id := f.add(t, lset, model.Sample{T: ms(-8 * time.Hour), V: 1})
	f.hot.Now = func() time.Time { return now }
	got, err := f.q.Samples(context.Background(), []model.SeriesID{id}, ms(-12*time.Hour), ms(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got[id]) != 1 {
		t.Fatal("a chunk still in the hot tier must stay visible beyond hot_lookback")
	}
}

// Native histograms are returned by PromQL from both tiers.
func TestNativeHistograms(t *testing.T) {
	f := newFixture(t)
	lset := labels.FromStrings("__name__", "request_duration_seconds", "job", "api")
	hist := func(i int) *histogram.FloatHistogram {
		return &histogram.FloatHistogram{
			Schema: 0, Count: float64(10 * i), Sum: float64(5 * i), ZeroThreshold: 0.001,
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
			PositiveBuckets: []float64{float64(4 * i), float64(6 * i)}, // (0.5,1] and (1,2]
		}
	}
	var hot, cold []model.Sample
	for i := 1; i <= 10; i++ {
		s := model.Sample{T: ms(time.Duration(i-10) * time.Minute), H: hist(i)}
		if i <= 5 {
			cold = append(cold, s)
		} else {
			hot = append(hot, s)
		}
	}
	id := f.add(t, lset, hot...)
	f.persist(t, id, cold...)

	code, body := f.get(t, "/api/v1/query", url.Values{"query": {`histogram_count(request_duration_seconds)`}, "time": {fmt.Sprint(now.Unix())}})
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	if v := body["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["value"].([]any)[1]; v != "100" {
		t.Fatalf("histogram_count = %v, want 100", v)
	}
	// The rate spans both tiers: 10 observations per minute.
	code, body = f.get(t, "/api/v1/query", url.Values{"query": {`histogram_count(rate(request_duration_seconds[10m]))`}, "time": {fmt.Sprint(now.Unix())}})
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	v, _ := strconv.ParseFloat(body["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["value"].([]any)[1].(string), 64)
	if math.Abs(v-10.0/60) > 1e-9 {
		t.Fatalf("rate = %v, want 10/60", v)
	}
	// Raw histograms are rendered in the "histogram" field.
	code, body = f.get(t, "/api/v1/query", url.Values{"query": {`request_duration_seconds`}, "time": {fmt.Sprint(now.Unix())}})
	res := body["data"].(map[string]any)["result"].([]any)[0].(map[string]any)
	if _, ok := res["histogram"]; code != http.StatusOK || !ok {
		t.Fatalf("raw histogram not rendered: %d %v", code, res)
	}
}

// Histograms give the same results from both tiers, also when the engine
// overwrites histograms it was given: it does so with points that leave the
// range of a matrix selector when a sample falls on an evaluation timestamp.
// Histograms decoded from one chunk of the persistent tier share their spans,
// so handing them out uncopied mixed up the buckets of a range that spans two
// chunks with different layouts, or made the query fail.
func TestHistogramsFromBothTiersAgree(t *testing.T) {
	base := ms(-20*time.Minute) / 30_000 * 30_000 // on the evaluation steps
	// New histograms for every fixture: the in-memory hot store keeps them.
	makeSamples := func() []model.Sample {
		var samples []model.Sample
		for i := range 60 {
			h := &histogram.FloatHistogram{Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}, {Offset: 2, Length: 2}}}
			n := i
			if i >= 30 { // a counter reset to another layout starts a new chunk
				h.PositiveSpans, n = []histogram.Span{{Offset: 5, Length: 3}, {Offset: 1, Length: 3}}, i-30
			}
			for j := range countSpans(h.PositiveSpans) {
				h.PositiveBuckets = append(h.PositiveBuckets, float64(n+j))
				h.Count += float64(n + j)
			}
			h.Sum = h.Count
			samples = append(samples, model.Sample{T: base + int64(i)*15_000, H: h})
		}
		return samples
	}
	end := makeSamples()[59].T / 1000
	for _, q := range []string{
		`sum_over_time(%s[2m])`,
		`histogram_quantile(0.5, sum_over_time(%s[2m]))`,
		`histogram_quantile(0.5, rate(%s[2m]))`,
	} {
		results := map[string]string{}
		for _, tier := range []string{"hot", "cold"} {
			f := newFixture(t)
			lset, samples := labels.FromStrings("__name__", "h"), makeSamples()
			if tier == "hot" {
				f.add(t, lset, samples...)
			} else {
				f.persist(t, f.indexSeries(t, lset, samples), samples...)
			}
			code, body := f.get(t, "/api/v1/query_range", url.Values{"query": {fmt.Sprintf(q, "h")},
				"start": {fmt.Sprint(base / 1000)}, "end": {fmt.Sprint(end)}, "step": {"30"}})
			if code != http.StatusOK {
				t.Fatalf("%s from the %s tier: %d %v", q, tier, code, body)
			}
			b, err := json.Marshal(body["data"])
			if err != nil {
				t.Fatal(err)
			}
			results[tier] = string(b)
		}
		if results["hot"] != results["cold"] {
			t.Errorf("%s differs between tiers:\nhot  %s\ncold %s", q, results["hot"], results["cold"])
		}
	}
}

func countSpans(spans []histogram.Span) int {
	n := 0
	for _, s := range spans {
		n += int(s.Length)
	}
	return n
}

// The iterator copies histograms, so that the engine can overwrite them.
func TestIteratorCopiesHistograms(t *testing.T) {
	h := &histogram.FloatHistogram{Count: 1, PositiveSpans: []histogram.Span{{Length: 1}}, PositiveBuckets: []float64{1}}
	stored := h.Copy()
	it := copyingIterator{promstorage.NewListSeriesIterator(sampleList{{T: 1, H: stored}, {T: 2, V: 1}})}
	if it.Next() != chunkenc.ValFloatHistogram {
		t.Fatal("expected a histogram")
	}
	_, got := it.AtFloatHistogram(nil)
	got.PositiveSpans[0].Offset, got.PositiveBuckets[0] = 7, 7
	into := &histogram.FloatHistogram{PositiveSpans: make([]histogram.Span, 1), PositiveBuckets: make([]float64, 1)}
	if _, got = it.AtFloatHistogram(into); got != into || !into.Equals(h) {
		t.Fatalf("AtFloatHistogram(fh) = %v, want a copy of %v in fh", got, h)
	}
	into.PositiveSpans[0].Offset, into.PositiveBuckets[0] = 9, 9
	if !stored.Equals(h) {
		t.Fatalf("the stored histogram was modified: %v", stored)
	}
}
