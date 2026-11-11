package model

import (
	"errors"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

func TestCanonicalizeSortsAndDropsEmpty(t *testing.T) {
	in := labels.New(
		labels.Label{Name: "zone", Value: "a"},
		labels.Label{Name: "__name__", Value: "cpu"},
		labels.Label{Name: "empty", Value: ""},
		labels.Label{Name: "host", Value: "h1"},
	)
	got, err := Canonicalize(in)
	if err != nil {
		t.Fatal(err)
	}
	want := labels.FromStrings("__name__", "cpu", "host", "h1", "zone", "a")
	if !labels.Equal(got, want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCanonicalizeErrors(t *testing.T) {
	cases := []struct {
		name string
		in   labels.Labels
		want error
	}{
		{"no name", labels.FromStrings("host", "a"), ErrNoMetricName},
		{"invalid utf8", labels.FromStrings("__name__", "x", "h", "\xff"), ErrInvalidLabel},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Canonicalize(c.in); !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
		})
	}
}

func TestIDIndependentOfInputOrder(t *testing.T) {
	a, _ := Canonicalize(labels.New(
		labels.Label{Name: "__name__", Value: "mem"},
		labels.Label{Name: "dc", Value: "a"},
		labels.Label{Name: "rack", Value: "1"},
	))
	b, _ := Canonicalize(labels.New(
		labels.Label{Name: "rack", Value: "1"},
		labels.Label{Name: "__name__", Value: "mem"},
		labels.Label{Name: "dc", Value: "a"},
	))
	if ID(a) != ID(b) {
		t.Fatalf("IDs differ: %s vs %s", ID(a), ID(b))
	}
}

// The series ID is part of the storage format and must never change.
func TestIDGolden(t *testing.T) {
	lset := labels.FromStrings("__name__", "up", "instance", "localhost:9090", "job", "prometheus")
	const want = SeriesID(0xf28c8afcd224e55f)
	if got := ID(lset); got != want {
		t.Fatalf("ID changed: got %#x, want %#x", uint64(got), uint64(want))
	}
}

func TestCheckIsIndependentOfID(t *testing.T) {
	a := labels.FromStrings("__name__", "up", "job", "a")
	b := labels.FromStrings("__name__", "up", "job", "b")
	if Check(a) == uint64(ID(a)) {
		t.Fatal("Check must not equal ID")
	}
	if Check(a) == Check(b) || Check(a) != Check(a.Copy()) {
		t.Fatal("Check must be a deterministic hash of the labels")
	}
}

func TestIDDistinguishesBoundaries(t *testing.T) {
	a := labels.FromStrings("__name__", "a", "b", "c")
	b := labels.FromStrings("__name__", "ab", "", "c")
	if ID(a) == ID(b) {
		t.Fatal("label boundaries must be part of the hash input")
	}
}

func TestWindows(t *testing.T) {
	l := Layout{Shards: 16, ChunkRange: 3_600_000, Grace: 600_000}
	ids := []SeriesID{0, 1, 3_599_999, 0xdeadbeefcafebabe}
	for _, id := range ids {
		for _, ts := range []int64{0, 1, 1_700_000_000_000, 1_700_003_599_999, -5} {
			ws := l.WindowStart(id, ts)
			if ts < ws || ts >= ws+l.ChunkRange {
				t.Fatalf("id=%d t=%d: window [%d,%d) does not contain t", id, ts, ws, ws+l.ChunkRange)
			}
			if (ws-int64(uint64(id)%uint64(l.ChunkRange)))%l.ChunkRange != 0 {
				t.Fatalf("id=%d: window %d not aligned to series offset", id, ws)
			}
		}
		ws := l.Windows(id, 1_700_000_000_000, 1_700_010_000_000)
		if len(ws) < 3 || len(ws) > 4 {
			t.Fatalf("unexpected window count %d", len(ws))
		}
		for i := 1; i < len(ws); i++ {
			if ws[i]-ws[i-1] != l.ChunkRange {
				t.Fatalf("windows not contiguous: %v", ws)
			}
		}
		if s := l.Shard(id); s < 0 || s >= l.Shards {
			t.Fatalf("shard %d out of range", s)
		}
	}
	if got := l.Due(1000); got != 1000+l.ChunkRange+l.Grace {
		t.Fatalf("Due = %d", got)
	}
}

func TestWindowsAreStaggered(t *testing.T) {
	l := Layout{Shards: 1, ChunkRange: 3_600_000}
	seen := map[int64]bool{}
	for i := range 100 {
		id := ID(labels.FromStrings("__name__", "m", "i", string(rune('a'+i%26)), "n", string(rune('a'+i/26))))
		seen[l.WindowStart(id, 1_700_000_000_000)] = true
	}
	if len(seen) < 90 {
		t.Fatalf("windows are not spread across series: %d distinct starts", len(seen))
	}
}

func TestDays(t *testing.T) {
	if Day(0) != 0 || Day(DayMillis-1) != 0 || Day(DayMillis) != 1 || Day(-1) != -1 {
		t.Fatal("Day boundaries are wrong")
	}
	if got := Days(DayMillis-1, 3*DayMillis); len(got) != 4 || got[0] != 0 || got[3] != 3 {
		t.Fatalf("Days = %v", got)
	}
	if Days(10, 5) != nil {
		t.Fatal("empty range must return nil")
	}
}

// A NaN zero threshold made PromQL loop forever when it added the histogram
// to another one; other non-finite or negative thresholds make no sense.
func TestValidateHistogramZeroThreshold(t *testing.T) {
	for _, zt := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1} {
		h := &histogram.FloatHistogram{Schema: 0, ZeroThreshold: zt}
		if ValidateHistogram(h) == nil || ValidateHistogramLayout(h) == nil {
			t.Errorf("zero threshold %v accepted", zt)
		}
	}
	for _, zt := range []float64{0, math.Ldexp(1, -128), 0.001, 1e300} {
		if err := ValidateHistogram(&histogram.FloatHistogram{Schema: 0, ZeroThreshold: zt}); err != nil {
			t.Errorf("zero threshold %v: %v", zt, err)
		}
	}
}

// Bucket indexes are int32: since Prometheus 3.16, whose validation
// ValidateHistogram applies, a span that reaches past math.MaxInt32 makes
// the histogram invalid (Prometheus stored such histograms with corrupted
// buckets).
func TestValidateHistogramSpanIndex(t *testing.T) {
	last := &histogram.FloatHistogram{Schema: 0, Count: 2,
		PositiveSpans: []histogram.Span{{Offset: math.MaxInt32 - 1, Length: 2}}, PositiveBuckets: []float64{1, 1}}
	if err := ValidateHistogram(last); err != nil {
		t.Errorf("last bucket at index math.MaxInt32: %v", err)
	}
	past := &histogram.FloatHistogram{Schema: 0, Count: 2,
		PositiveSpans: []histogram.Span{{Offset: math.MaxInt32, Length: 2}}, PositiveBuckets: []float64{1, 1}}
	if ValidateHistogram(past) == nil || ValidateHistogramLayout(past) == nil {
		t.Error("bucket index past math.MaxInt32 accepted")
	}
}

// The layout of a persistent-tier chunk may have more buckets than one
// histogram, but not more spans or custom bounds.
func TestValidateHistogramLayout(t *testing.T) {
	n := MaxHistogramBuckets + 1
	many := &histogram.FloatHistogram{Schema: 0, PositiveSpans: []histogram.Span{{Length: uint32(n)}}, PositiveBuckets: make([]float64, n)}
	if ValidateHistogram(many) == nil {
		t.Error("ValidateHistogram accepted too many buckets")
	}
	if err := ValidateHistogramLayout(many); err != nil {
		t.Errorf("ValidateHistogramLayout: %v", err)
	}
	spans := &histogram.FloatHistogram{Schema: 0, PositiveSpans: make([]histogram.Span, n)}
	if ValidateHistogramLayout(spans) == nil {
		t.Error("ValidateHistogramLayout accepted too many spans")
	}
}
