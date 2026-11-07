package ingest

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

	"github.com/yuuki/xtsdb/internal/model"
)

// Lines received over TCP and UDP are untrusted. A parsed line has exactly
// one finite or infinite sample with a timestamp that converts to
// milliseconds without overflow, and rendering it back gives the same
// series and sample.
func FuzzParseGraphiteLine(f *testing.F) {
	for _, line := range []string{
		"servers.web01.cpu 12.5 1700000000",
		"servers.web01.load;dc=tokyo;rack=a1 0.75 1700000000.123",
		"cpu.user;host=web01 12.5",
		"m nan 1700000000",
		"m -0 -1",
		"m +Inf 1e11",
		"m 0x1p-2 99999999999.9999",
		"m;a=b=c;d= 1 0",
		"m;;a=b 1 1",
		"m;__name__=x 1 1",
		";a=b 1 1",
		"m 1 2 3",
		"m 1 1e12",
	} {
		f.Add(line)
	}
	now := time.Unix(1_700_000_000, 0)
	f.Fuzz(func(t *testing.T, line string) {
		s, ok, err := ParseGraphiteLine(line, now)
		if err != nil || !ok {
			if ok {
				t.Fatalf("ok with error %v", err)
			}
			return
		}
		if len(s.Samples) != 1 {
			t.Fatalf("%d samples", len(s.Samples))
		}
		smp := s.Samples[0]
		if math.IsNaN(smp.V) {
			t.Fatal("NaN value accepted")
		}
		if smp.T != now.UnixMilli() && (smp.T < 0 || smp.T > maxGraphiteSeconds*1000) {
			t.Fatalf("timestamp %d out of range", smp.T)
		}
		if s.Labels.Get(model.MetricName) == "" {
			t.Fatal("no metric name")
		}
		var rendered strings.Builder
		s.Labels.Range(func(l labels.Label) {
			if l.Name == "" || strings.ContainsAny(l.Name, ";=") || strings.ContainsFunc(l.Name+l.Value, unicode.IsSpace) {
				t.Fatalf("invalid label %q=%q", l.Name, l.Value)
			}
			if l.Name == model.MetricName {
				rendered.WriteString(l.Value)
				return
			}
			rendered.WriteString(";" + l.Name + "=" + l.Value)
		})
		rendered.WriteString(" " + strconv.FormatFloat(smp.V, 'g', -1, 64))
		rendered.WriteString(" " + strconv.FormatFloat(float64(smp.T)/1000, 'f', 3, 64))
		again, ok, err := ParseGraphiteLine(rendered.String(), now)
		if err != nil || !ok {
			t.Fatalf("re-parse of %q: %v %v", rendered.String(), ok, err)
		}
		got := again.Samples[0]
		if !labels.Equal(again.Labels, s.Labels) || got.T != smp.T || math.Float64bits(got.V) != math.Float64bits(smp.V) {
			t.Fatalf("round trip of %q via %q: %v %v, want %v %v", line, rendered.String(), again.Labels, got, s.Labels, smp)
		}
	})
}

// fuzzLimits are small so that the fuzzer reaches them with small inputs.
var fuzzLimits = Limits{MaxSeries: 4, MaxSamples: 32}

// Remote write bodies are untrusted. A body that prescan accepts decodes into
// no more elements than the limits allow, so that decoding cannot allocate
// much more than the size of the body, and converting it never panics.
func FuzzRemoteWrite(f *testing.F) {
	h := testHistogram(1)
	v1 := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels:     []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "a"}},
			Samples:    []prompb.Sample{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}},
			Exemplars:  []prompb.Exemplar{{Labels: []prompb.Label{{Name: "trace_id", Value: "x"}}, Value: 1, Timestamp: 1}},
			Histograms: []prompb.Histogram{prompb.FromIntHistogram(3, h), prompb.FromFloatHistogram(4, h.ToFloat(nil))},
		}},
		Metadata: []prompb.MetricMetadata{{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: "up", Help: "h"}},
	}
	st := writev2.NewSymbolTable()
	v2 := &writev2.Request{Timeseries: []writev2.TimeSeries{{
		LabelsRefs: st.SymbolizeLabels(labels.FromStrings("__name__", "up", "job", "a"), nil),
		Samples:    []writev2.Sample{{Timestamp: 1, Value: 1}},
		Histograms: []writev2.Histogram{writev2.FromIntHistogram(0, 2, h), writev2.FromFloatHistogram(0, 3, h.ToFloat(nil))},
		Exemplars:  []writev2.Exemplar{{LabelsRefs: []uint32{1, 2}, Value: 1, Timestamp: 1}},
	}}, Symbols: st.Symbols()}
	b1, err := v1.Marshal()
	if err != nil {
		f.Fatal(err)
	}
	b2, err := v2.Marshal()
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range []struct {
		b  []byte
		v2 bool
	}{
		{nil, false}, {b1, false}, {b2, true}, {b1, true}, {b2, false},
		{bytes.Repeat([]byte{0x0a, 0x00}, 8), false},
		{bytes.Repeat([]byte{0x1a, 0x00}, 40), false},
		{[]byte{0x0a, 0x04, 0x1a, 0x02, 0x0a, 0x00}, false},
		{[]byte{0x2a, 0x02, 0x0a, 0x00}, true},
	} {
		f.Add(seed.b, seed.v2)
	}
	f.Fuzz(func(t *testing.T, body []byte, v2 bool) {
		if prescan(body, v2, fuzzLimits) != nil {
			return
		}
		var series, samples, other int
		if v2 {
			var req writev2.Request
			if req.Unmarshal(body) != nil {
				return
			}
			series, samples, other = countV2(&req)
		} else {
			var req prompb.WriteRequest
			if req.Unmarshal(body) != nil {
				return
			}
			series, samples, other = countV1(&req)
		}
		if series > fuzzLimits.MaxSeries || samples > fuzzLimits.MaxSamples || other > fuzzLimits.MaxSamples {
			t.Fatalf("prescan accepted %d series, %d samples and %d other elements, beyond %+v", series, samples, other, fuzzLimits)
		}
		var d decoded
		decode := d.v1
		if v2 {
			decode = d.v2
		}
		if decode(body) != nil {
			return
		}
		n := d.invalid
		for _, s := range d.batch {
			n += len(s.Samples)
		}
		if n > samples+other {
			t.Fatalf("decoded %d samples from %d samples and %d other elements", n, samples, other)
		}
	})
}

func countV1(req *prompb.WriteRequest) (series, samples, other int) {
	series, other = len(req.Timeseries), len(req.Metadata)
	for _, ts := range req.Timeseries {
		samples += len(ts.Samples)
		other += len(ts.Labels) + len(ts.Exemplars)
		for _, ex := range ts.Exemplars {
			other += len(ex.Labels)
		}
		for _, h := range ts.Histograms {
			other += 1 + len(h.NegativeSpans) + len(h.NegativeDeltas) + len(h.NegativeCounts) +
				len(h.PositiveSpans) + len(h.PositiveDeltas) + len(h.PositiveCounts) + len(h.CustomValues)
		}
	}
	return series, samples, other
}

// countV2 counts like countV1. The label references of exemplars are left
// out: they are packed 32-bit integers, which decoding inflates at most
// fourfold.
func countV2(req *writev2.Request) (series, samples, other int) {
	series, other = len(req.Timeseries), len(req.Symbols)
	for _, ts := range req.Timeseries {
		samples += len(ts.Samples)
		other += len(ts.LabelsRefs) + len(ts.Exemplars)
		for _, h := range ts.Histograms {
			other += 1 + len(h.NegativeSpans) + len(h.NegativeDeltas) + len(h.NegativeCounts) +
				len(h.PositiveSpans) + len(h.PositiveDeltas) + len(h.PositiveCounts) + len(h.CustomValues)
		}
	}
	return series, samples, other
}
