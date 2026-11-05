package model

import (
	"errors"
	"testing"

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
