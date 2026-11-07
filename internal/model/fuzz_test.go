package model

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/prometheus/prometheus/model/labels"
)

// fuzzLabels splits s into label names and values at 0xff, which valid
// UTF-8 never contains, so that every valid label set can be expressed.
func fuzzLabels(s string, reverse bool) labels.Labels {
	parts := strings.Split(s, "\xff")
	b := labels.NewScratchBuilder(len(parts) / 2)
	var pairs [][2]string
	for i := 0; i+1 < len(parts); i += 2 {
		pairs = append(pairs, [2]string{parts[i], parts[i+1]})
	}
	if reverse {
		slices.Reverse(pairs)
	}
	for _, p := range pairs {
		b.Add(p[0], p[1])
	}
	return b.Labels()
}

// Labels of incoming series are untrusted and determine the series ID,
// which is part of the storage format. Canonical labels are valid, sorted
// and unique, canonicalizing them again changes nothing, and neither the
// outcome nor the ID depends on the order the labels arrived in.
func FuzzCanonicalize(f *testing.F) {
	for _, s := range []string{
		"__name__\xffup\xffjob\xffa",
		"zone\xffa\xff__name__\xffcpu\xffempty\xff\xffhost\xffh1",
		"__name__\xffm\xffa\xffx\xffa\xffy",
		"__name__\xffm\xffa\xff\xffa\xffy",
		"__name__\xff\xc3\x28",
		"\xffv\xff__name__\xffm",
		"job\xffa",
		"__name__\xff" + strings.Repeat("x", MaxLabelLength+1),
		"",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		in := fuzzLabels(s, false)
		out, err := Canonicalize(in)
		rev, revErr := Canonicalize(fuzzLabels(s, true))
		if (err == nil) != (revErr == nil) {
			t.Fatalf("outcome depends on the order of %v: %v, reversed %v", in, err, revErr)
		}
		if err != nil {
			for _, known := range []error{ErrNoMetricName, ErrInvalidLabel, ErrDuplicateLabel, ErrTooManyLabels, ErrLabelValueTooLong} {
				if errors.Is(err, known) {
					return
				}
			}
			t.Fatalf("unexpected error %v", err)
		}
		if !labels.Equal(out, rev) || ID(out) != ID(rev) || Check(out) != Check(rev) {
			t.Fatalf("%v and its reverse canonicalize to %v and %v", in, out, rev)
		}
		if out.Len() > MaxLabelsPerSeries || out.Get(MetricName) == "" {
			t.Fatalf("invalid canonical labels %v", out)
		}
		prev := ""
		out.Range(func(l labels.Label) {
			switch {
			case l.Name <= prev:
				t.Fatalf("labels not sorted or not unique: %v", out)
			case l.Value == "" || len(l.Name) > MaxLabelLength || len(l.Value) > MaxLabelLength:
				t.Fatalf("invalid label %q=%q", l.Name, l.Value)
			case !utf8.ValidString(l.Name) || !utf8.ValidString(l.Value):
				t.Fatalf("label %q=%q is not UTF-8", l.Name, l.Value)
			}
			prev = l.Name
		})
		again, err := Canonicalize(out)
		if err != nil || !labels.Equal(again, out) {
			t.Fatalf("canonicalizing %v again gives %v, %v", out, again, err)
		}
	})
}

// Window starts, shards and days decide where samples are stored. For any
// timestamp within ±2^56 ms (2 million years, where days still fit
// their 32 bits) and any layout, the window
// of a sample contains it, starts on the boundary of its series, and is
// among the windows that a read of its time range visits.
func FuzzLayout(f *testing.F) {
	f.Add(uint64(0xdeadbeef00000002), int64(1_700_000_000_000), int64(3_600_000), uint16(256))
	f.Add(uint64(1), int64(-1), int64(1), uint16(1))
	f.Add(uint64(1<<63), int64(-1<<56), int64(1<<40), uint16(65535))
	f.Add(uint64(7919), int64(1<<56), int64(86_400_000), uint16(3))
	f.Fuzz(func(t *testing.T, id uint64, ts, chunkRange int64, shards uint16) {
		const bound = 1 << 56
		if ts < -bound || ts > bound || chunkRange <= 0 || chunkRange > 1<<40 || shards == 0 {
			return
		}
		l := Layout{Shards: int(shards), ChunkRange: chunkRange}
		sid := SeriesID(id)
		if s := l.Shard(sid); s < 0 || s >= l.Shards {
			t.Fatalf("shard %d of %d", s, l.Shards)
		}
		start := l.WindowStart(sid, ts)
		if start > ts || ts-start >= chunkRange {
			t.Fatalf("window [%d, %d) does not contain %d", start, start+chunkRange, ts)
		}
		if l.WindowStart(sid, start) != start || l.WindowStart(sid, start+chunkRange-1) != start {
			t.Fatalf("window starting at %d is not stable", start)
		}
		if w := l.Windows(sid, ts, ts); len(w) != 1 || w[0] != start {
			t.Fatalf("windows of [%d, %d]: %v, want [%d]", ts, ts, w, start)
		}
		if d := Days(ts, ts); len(d) != 1 || d[0] != Day(ts) || int64(Day(ts))*DayMillis > ts || ts-int64(Day(ts))*DayMillis >= DayMillis {
			t.Fatalf("day %d of %d", Day(ts), ts)
		}
	})
}
