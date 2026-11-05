package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// APIOptions configures the HTTP API.
type APIOptions struct {
	LookbackDelta time.Duration
	Timeout       time.Duration
	MaxSamples    int
	Registerer    prometheus.Registerer
	Logger        *slog.Logger
	BuildInfo     map[string]string
}

// API serves a Prometheus-compatible HTTP API, so that Grafana and other
// Prometheus clients can query xtsdb directly.
type API struct {
	q      *Queryable
	engine *promql.Engine
	parser parser.Parser
	opts   APIOptions
	now    func() time.Time
}

// maxPointsPerSeries mirrors the Prometheus range query resolution limit.
const maxPointsPerSeries = 11000

// metadataDefaultRange is used by the series and label endpoints when the
// client sends no start time.
const metadataDefaultRange = 24 * time.Hour

// NewAPI creates an API.
func NewAPI(q *Queryable, opts APIOptions) *API {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	p := parser.NewParser(parser.Options{})
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:                   opts.Logger.With("component", "promql"),
		Reg:                      opts.Registerer,
		MaxSamples:               opts.MaxSamples,
		Timeout:                  opts.Timeout,
		LookbackDelta:            opts.LookbackDelta,
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
		Parser:                   p,
	})
	return &API{q: q, engine: engine, parser: p, opts: opts, now: time.Now}
}

// Register adds the API routes to mux.
func (a *API) Register(mux *http.ServeMux, wrap func(http.Handler) http.Handler) {
	route := func(path string, h func(*http.Request) apiResult) {
		mux.Handle(path, wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodPost {
				w.Header().Set("Allow", "GET, POST")
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if err := r.ParseForm(); err != nil {
				writeResult(w, a.opts.Logger, apiResult{err: badData(err)})
				return
			}
			writeResult(w, a.opts.Logger, h(r))
		})))
	}
	route("/api/v1/query", a.query)
	route("/api/v1/query_range", a.queryRange)
	route("/api/v1/series", a.series)
	route("/api/v1/labels", a.labelNames)
	route("/api/v1/label/{name}/values", a.labelValues)
	route("/api/v1/status/buildinfo", func(*http.Request) apiResult { return apiResult{data: a.opts.BuildInfo} })
	// Endpoints Grafana probes. xtsdb has no rules, alerts, exemplars or
	// metadata, so they return empty results.
	route("/api/v1/metadata", func(*http.Request) apiResult { return apiResult{data: map[string]any{}} })
	route("/api/v1/rules", func(*http.Request) apiResult { return apiResult{data: map[string]any{"groups": []any{}}} })
	route("/api/v1/alerts", func(*http.Request) apiResult { return apiResult{data: map[string]any{"alerts": []any{}}} })
	route("/api/v1/query_exemplars", func(*http.Request) apiResult { return apiResult{data: []any{}} })
}

type apiError struct {
	typ  string
	code int
	err  error
}

func (e *apiError) Error() string { return e.err.Error() }

func badData(err error) *apiError {
	return &apiError{typ: "bad_data", code: http.StatusBadRequest, err: err}
}

type apiResult struct {
	data     any
	err      *apiError
	warnings []string
	infos    []string
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}

func (a *API) query(r *http.Request) apiResult {
	ts := a.now()
	if s := r.FormValue("time"); s != "" {
		t, err := parseTime(s)
		if err != nil {
			return apiResult{err: badData(fmt.Errorf("invalid parameter \"time\": %w", err))}
		}
		ts = t
	}
	ctx, cancel, aerr := a.withTimeout(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	defer cancel()
	qry, err := a.engine.NewInstantQuery(ctx, a.q, nil, r.FormValue("query"), ts)
	if err != nil {
		return apiResult{err: badData(err)}
	}
	return a.exec(ctx, qry, r.FormValue("query"))
}

func (a *API) queryRange(r *http.Request) apiResult {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return apiResult{err: badData(fmt.Errorf("invalid parameter \"start\": %w", err))}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return apiResult{err: badData(fmt.Errorf("invalid parameter \"end\": %w", err))}
	}
	if end.Before(start) {
		return apiResult{err: badData(errors.New("end timestamp must not be before start time"))}
	}
	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return apiResult{err: badData(fmt.Errorf("invalid parameter \"step\": %w", err))}
	}
	if step <= 0 {
		return apiResult{err: badData(errors.New("zero or negative query resolution step widths are not accepted"))}
	}
	if end.Sub(start)/step > maxPointsPerSeries {
		return apiResult{err: badData(fmt.Errorf("exceeded maximum resolution of %d points per timeseries", maxPointsPerSeries))}
	}
	ctx, cancel, aerr := a.withTimeout(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	defer cancel()
	qry, err := a.engine.NewRangeQuery(ctx, a.q, nil, r.FormValue("query"), start, end, step)
	if err != nil {
		return apiResult{err: badData(err)}
	}
	return a.exec(ctx, qry, r.FormValue("query"))
}

func (a *API) withTimeout(r *http.Request) (context.Context, context.CancelFunc, *apiError) {
	ctx := r.Context()
	if s := r.FormValue("timeout"); s != "" {
		d, err := parseDuration(s)
		if err != nil {
			return nil, nil, badData(fmt.Errorf("invalid parameter \"timeout\": %w", err))
		}
		c, cancel := context.WithTimeout(ctx, d)
		return c, cancel, nil
	}
	c, cancel := context.WithCancel(ctx)
	return c, cancel, nil
}

func (a *API) exec(ctx context.Context, qry promql.Query, qs string) apiResult {
	defer qry.Close()
	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiResult{err: classify(res.Err)}
	}
	warnings, infos := res.Warnings.AsStrings(qs, 10, 10)
	return apiResult{
		data:     queryData{ResultType: res.Value.Type(), Result: res.Value},
		warnings: warnings,
		infos:    infos,
	}
}

func classify(err error) *apiError {
	var (
		timeout  promql.ErrQueryTimeout
		canceled promql.ErrQueryCanceled
		tooMany  promql.ErrTooManySamples
	)
	switch {
	case errors.As(err, &timeout), errors.Is(err, context.DeadlineExceeded):
		return &apiError{typ: "timeout", code: http.StatusServiceUnavailable, err: err}
	case errors.As(err, &canceled), errors.Is(err, context.Canceled):
		return &apiError{typ: "canceled", code: 499, err: err}
	case errors.Is(err, ErrNoPositiveMatcher):
		return badData(err)
	case errors.Is(err, ErrTooManySeries), errors.As(err, &tooMany):
		return &apiError{typ: "execution", code: http.StatusUnprocessableEntity, err: err}
	}
	var storageErr promql.ErrStorage
	if errors.As(err, &storageErr) {
		return &apiError{typ: "internal", code: http.StatusInternalServerError, err: err}
	}
	return &apiError{typ: "execution", code: http.StatusUnprocessableEntity, err: err}
}

// timeRange parses the optional start and end parameters of the metadata
// endpoints.
func (a *API) timeRange(r *http.Request) (int64, int64, *apiError) {
	end := a.now()
	if s := r.FormValue("end"); s != "" {
		t, err := parseTime(s)
		if err != nil {
			return 0, 0, badData(fmt.Errorf("invalid parameter \"end\": %w", err))
		}
		end = t
	}
	start := end.Add(-metadataDefaultRange)
	if s := r.FormValue("start"); s != "" {
		t, err := parseTime(s)
		if err != nil {
			return 0, 0, badData(fmt.Errorf("invalid parameter \"start\": %w", err))
		}
		start = t
	}
	if end.Before(start) {
		return 0, 0, badData(errors.New("end timestamp must not be before start timestamp"))
	}
	return start.UnixMilli(), end.UnixMilli(), nil
}

func (a *API) matcherSets(r *http.Request) ([][]*labels.Matcher, *apiError) {
	sets, err := a.parser.ParseMetricSelectors(r.Form["match[]"])
	if err != nil {
		return nil, badData(err)
	}
	return sets, nil
}

func limitParam(r *http.Request) (int, *apiError) {
	s := r.FormValue("limit")
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, badData(fmt.Errorf("invalid parameter \"limit\": %q", s))
	}
	return n, nil
}

func (a *API) series(r *http.Request) apiResult {
	if len(r.Form["match[]"]) == 0 {
		return apiResult{err: badData(errors.New("no match[] parameter provided"))}
	}
	mint, maxt, aerr := a.timeRange(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	sets, aerr := a.matcherSets(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	limit, aerr := limitParam(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	seen := make(map[string]struct{})
	out := []labels.Labels{}
	var warnings []string
	for _, ms := range sets {
		refs, err := a.q.Series(r.Context(), mint, maxt, ms...)
		if err != nil {
			return apiResult{err: classify(err)}
		}
		for _, ref := range refs {
			key := ref.lset.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, ref.lset)
		}
	}
	slices.SortFunc(out, labels.Compare)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		warnings = append(warnings, "results truncated due to limit")
	}
	return apiResult{data: out, warnings: warnings}
}

func (a *API) labelNames(r *http.Request) apiResult {
	return a.labelStrings(r, func(ctx context.Context, mint, maxt int64, limit int, ms []*labels.Matcher) ([]string, error) {
		return a.q.LabelNames(ctx, mint, maxt, limit, ms...)
	})
}

func (a *API) labelValues(r *http.Request) apiResult {
	name := model.UnescapeName(r.PathValue("name"), model.ValueEncodingEscaping)
	if name == "" {
		return apiResult{err: badData(errors.New("invalid label name"))}
	}
	return a.labelStrings(r, func(ctx context.Context, mint, maxt int64, limit int, ms []*labels.Matcher) ([]string, error) {
		return a.q.LabelValues(ctx, mint, maxt, name, limit, ms...)
	})
}

func (a *API) labelStrings(r *http.Request, get func(context.Context, int64, int64, int, []*labels.Matcher) ([]string, error)) apiResult {
	mint, maxt, aerr := a.timeRange(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	sets, aerr := a.matcherSets(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	limit, aerr := limitParam(r)
	if aerr != nil {
		return apiResult{err: aerr}
	}
	if len(sets) == 0 {
		sets = [][]*labels.Matcher{nil}
	}
	set := make(map[string]struct{})
	for _, ms := range sets {
		vals, err := get(r.Context(), mint, maxt, 0, ms)
		if err != nil {
			return apiResult{err: classify(err)}
		}
		for _, v := range vals {
			set[v] = struct{}{}
		}
	}
	return apiResult{data: sortedLimit(set, limit)}
}

type response struct {
	Status    string   `json:"status"`
	Data      any      `json:"data,omitempty"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
	Warnings  []string `json:"warnings,omitempty"`
	Infos     []string `json:"infos,omitempty"`
}

func writeResult(w http.ResponseWriter, logger *slog.Logger, res apiResult) {
	w.Header().Set("Content-Type", "application/json")
	resp := response{Status: "success", Data: res.data, Warnings: res.warnings, Infos: res.infos}
	code := http.StatusOK
	if res.err != nil {
		resp = response{Status: "error", ErrorType: res.err.typ, Error: res.err.Error()}
		code = res.err.code
		if code >= 500 {
			logger.Warn("query failed", "err", res.err)
		}
	}
	b, err := json.Marshal(resp)
	if err != nil {
		logger.Error("encoding response failed", "err", err)
		http.Error(w, `{"status":"error","errorType":"internal","error":"encoding response failed"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

// parseTime accepts Unix timestamps in seconds (with optional fraction) and
// RFC 3339 times, like Prometheus.
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("missing value")
	}
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		if math.IsNaN(t) || math.IsInf(t, 0) || math.Abs(t) > 1e11 {
			return time.Time{}, fmt.Errorf("timestamp %q out of range", s)
		}
		sec, frac := math.Modf(t)
		frac = math.Round(frac*1000) / 1000
		return time.Unix(int64(sec), int64(frac*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

// parseDuration accepts seconds (with optional fraction) and Prometheus
// duration strings.
func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) || math.IsNaN(ts) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration: it overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(strings.TrimSpace(s)); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
