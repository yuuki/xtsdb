package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/chunk"
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
	if _, err := f.hot.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: samples}}); err != nil {
		t.Fatal(err)
	}
	return id
}

// persist writes samples directly to the cold tier.
func (f *fixture) persist(t *testing.T, id model.SeriesID, samples ...model.Sample) {
	t.Helper()
	data, err := chunk.EncodeXOR(samples)
	if err != nil {
		t.Fatal(err)
	}
	start := layout.WindowStart(id, samples[0].T)
	if err := f.cold.WriteChunk(context.Background(), storage.ColdChunk{
		Series: id, Start: start, Fragment: 1, MaxT: samples[len(samples)-1].T,
		Count: len(samples), Encoding: chunk.EncodingXOR, Data: data,
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
		refs, err := f.q.Series(ctx, ms(-time.Hour), ms(0), c.matchers...)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if len(refs) != c.want {
			t.Errorf("case %d: got %d series, want %d", i, len(refs), c.want)
		}
	}
	_, err := f.q.Series(ctx, ms(-time.Hour), ms(0), labels.MustNewMatcher(labels.MatchNotEqual, "job", "x"))
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
	for _, s := range []string{"", "abc", "NaN", "1e300"} {
		if _, err := parseTime(s); err == nil {
			t.Errorf("parseTime(%q) should fail", s)
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
}
