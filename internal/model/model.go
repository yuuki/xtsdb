// Package model defines the series identity and time partitioning rules
// shared by every xtsdb component. Everything here is part of the storage
// format: changing it changes where data is written.
package model

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	pmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

// Sample is a single sample. T is a Unix timestamp in milliseconds. A
// sample is either a float (V) or, when H is set, a native histogram;
// integer histograms are held as float histograms, which represent them
// exactly.
type Sample struct {
	T int64
	V float64
	H *histogram.FloatHistogram
}

// IsHistogram reports whether the sample is a native histogram.
func (s Sample) IsHistogram() bool { return s.H != nil }

// MaxHistogramBuckets bounds the buckets, spans and custom bounds of one
// native histogram (Prometheus histograms have at most a few hundred
// buckets), so that a single histogram stays far below the size limits of a
// hot chunk and of a cold-tier row. A larger one could be stored in Redis
// but never flushed.
const MaxHistogramBuckets = 16384

// ValidateHistogram checks that a histogram is consistent and not too large.
func ValidateHistogram(h *histogram.FloatHistogram) error {
	switch {
	case len(h.PositiveBuckets)+len(h.NegativeBuckets) > MaxHistogramBuckets:
		return fmt.Errorf("histogram has %d buckets, more than %d", len(h.PositiveBuckets)+len(h.NegativeBuckets), MaxHistogramBuckets)
	case len(h.PositiveSpans)+len(h.NegativeSpans) > MaxHistogramBuckets:
		return fmt.Errorf("histogram has %d spans, more than %d", len(h.PositiveSpans)+len(h.NegativeSpans), MaxHistogramBuckets)
	case len(h.CustomValues) > MaxHistogramBuckets:
		return fmt.Errorf("histogram has %d custom bounds, more than %d", len(h.CustomValues), MaxHistogramBuckets)
	}
	return h.Validate()
}

// MetricName is the label that holds the metric name.
const MetricName = pmodel.MetricNameLabel

// SeriesID identifies a series. It is the xxhash64 of its canonical labels.
type SeriesID uint64

// String renders the ID as fixed-width hex, as used in Redis keys.
func (id SeriesID) String() string { return fmt.Sprintf("%016x", uint64(id)) }

// Errors returned by Canonicalize.
var (
	ErrNoMetricName      = errors.New("series has no __name__ label")
	ErrInvalidLabel      = errors.New("invalid label")
	ErrDuplicateLabel    = errors.New("duplicate label name")
	ErrTooManyLabels     = errors.New("too many labels")
	ErrLabelValueTooLong = errors.New("label name or value too long")
)

// Limits applied to incoming series to protect the index.
const (
	MaxLabelsPerSeries = 64
	MaxLabelLength     = 2048
)

// Canonicalize validates a label set and returns it sorted with empty values
// removed. The returned labels determine the series identity, so the same
// series always maps to the same ID regardless of the order its labels
// arrived in.
func Canonicalize(lset labels.Labels) (labels.Labels, error) {
	b := labels.NewScratchBuilder(lset.Len())
	var err error
	lset.Range(func(l labels.Label) {
		if err != nil || l.Value == "" {
			return
		}
		switch {
		case l.Name == "" || !utf8.ValidString(l.Name) || !utf8.ValidString(l.Value):
			err = fmt.Errorf("%w: %q=%q", ErrInvalidLabel, l.Name, l.Value)
			return
		case len(l.Name) > MaxLabelLength || len(l.Value) > MaxLabelLength:
			err = fmt.Errorf("%w: %q", ErrLabelValueTooLong, l.Name)
			return
		}
		b.Add(l.Name, l.Value)
	})
	if err != nil {
		return labels.EmptyLabels(), err
	}
	b.Sort()
	out := b.Labels()
	if out.Len() > MaxLabelsPerSeries {
		return labels.EmptyLabels(), fmt.Errorf("%w: %d > %d", ErrTooManyLabels, out.Len(), MaxLabelsPerSeries)
	}
	if name, dup := out.HasDuplicateLabelNames(); dup {
		return labels.EmptyLabels(), fmt.Errorf("%w: %q", ErrDuplicateLabel, name)
	}
	if out.Get(MetricName) == "" {
		return labels.EmptyLabels(), ErrNoMetricName
	}
	return out, nil
}

// separator cannot appear in valid UTF-8, so it unambiguously delimits names
// and values in the hash input.
const separator = '\xff'

// ID computes the series ID of canonical labels. The hash input is fixed
// (name, 0xff, value, 0xff, ...) and does not depend on the in-memory label
// representation, so IDs are stable across builds and versions.
//
// Different label sets can share an ID (about n²/2⁶⁵ for n series). The
// index detects this when it records a series; see Check.
func ID(lset labels.Labels) SeriesID { return SeriesID(hash(lset, 0)) }

// checkSeed makes Check independent of ID.
const checkSeed = 0x9e3779b97f4a7c15

// Check is a second hash of canonical labels, independent of ID. Two label
// sets with the same ID almost certainly have different checks, which lets
// caches of known series tell colliding series apart without holding their
// labels. It is not part of the storage format.
func Check(lset labels.Labels) uint64 { return hash(lset, checkSeed) }

func hash(lset labels.Labels, seed uint64) uint64 {
	d := xxhash.NewWithSeed(seed)
	var buf [1]byte
	buf[0] = separator
	lset.Range(func(l labels.Label) {
		_, _ = d.WriteString(l.Name)
		_, _ = d.Write(buf[:])
		_, _ = d.WriteString(l.Value)
		_, _ = d.Write(buf[:])
	})
	return d.Sum64()
}

// Layout describes how series are spread over shards and how their samples
// are cut into chunks. All values are in milliseconds.
type Layout struct {
	Shards     int
	ChunkRange int64
	Grace      int64
}

// Shard returns the hot-tier shard of a series.
func (l Layout) Shard(id SeriesID) int {
	// Use the high bits: the low bits already pick the window offset.
	return int((uint64(id) >> 32) % uint64(l.Shards))
}

// offset staggers window boundaries per series so that chunks of different
// series do not all become due at the same instant.
func (l Layout) offset(id SeriesID) int64 {
	return int64(uint64(id) % uint64(l.ChunkRange))
}

// WindowStart returns the start of the chunk window containing t.
func (l Layout) WindowStart(id SeriesID, t int64) int64 {
	off := l.offset(id)
	return floorDiv(t-off, l.ChunkRange)*l.ChunkRange + off
}

// Due returns the time after which a window is closed: no more samples are
// accepted for it and it may be moved to the persistent tier.
func (l Layout) Due(windowStart int64) int64 {
	return windowStart + l.ChunkRange + l.Grace
}

// Windows returns the starts of all windows of a series that overlap
// [mint, maxt], in ascending order.
func (l Layout) Windows(id SeriesID, mint, maxt int64) []int64 {
	if maxt < mint {
		return nil
	}
	first := l.WindowStart(id, mint)
	last := l.WindowStart(id, maxt)
	out := make([]int64, 0, (last-first)/l.ChunkRange+1)
	for w := first; w <= last; w += l.ChunkRange {
		out = append(out, w)
	}
	return out
}

// DayMillis is the width of the partitions used by the persistent tier and
// the index. It is part of the storage format.
const DayMillis = 24 * 60 * 60 * 1000

// Day returns the day bucket of a timestamp.
func Day(t int64) int32 { return int32(floorDiv(t, DayMillis)) }

// Days returns every day bucket overlapping [mint, maxt].
func Days(mint, maxt int64) []int32 {
	if maxt < mint {
		return nil
	}
	first, last := Day(mint), Day(maxt)
	out := make([]int32, 0, last-first+1)
	for d := first; d <= last; d++ {
		out = append(out, d)
	}
	return out
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}
