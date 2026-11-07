package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/yuuki/xtsdb/internal/model"
)

// The chunkenc iterators trust the chunks they decode. A float histogram
// chunk starts with the bucket layout that all its histograms share, and the
// iterator allocates as many buckets as that layout claims before it reads
// any of them: a corrupt layout can claim billions, and the process runs out
// of memory, which recover cannot catch. checkHistogramChunk therefore reads
// the layout first. The format, from tsdb/chunkenc, is
//
//	samples   uint16, big endian
//	flags     one byte (the counter reset header)
//	layout    zero threshold, schema, positive spans, negative spans, and
//	          custom bounds with the custom buckets schema
//	samples   the first one holds its timestamp, then its count, zero count,
//	          sum and every bucket as raw 64-bit floats
//
// with the layout and the samples in a bit stream (see bitReader).

const histogramChunkHeaderLen = 3

// checkHistogramChunk returns ErrCorrupt unless the layout of the float
// histogram chunk b is one that model.ValidateHistogramLayout accepts (at
// most model.MaxHistogramBuckets spans and custom bounds, a valid schema and
// zero threshold, among others), and b is long enough for the buckets of its
// first histogram. The number of buckets is not limited otherwise: the
// layout of a chunk is the union of the layouts of its histograms, and
// earlier builds wrote chunks with more buckets than one histogram may have.
// Decoding corrupt data then takes about as much memory as valid data of the
// same size.
//
// The encoder runs the same check, so that it never writes a chunk that
// decoding refuses.
func checkHistogramChunk(b []byte) error {
	if len(b) < histogramChunkHeaderLen {
		return fmt.Errorf("%w: histogram chunk of %d bytes", ErrCorrupt, len(b))
	}
	if binary.BigEndian.Uint16(b) == 0 {
		return nil // the iterator reads nothing
	}
	r := bitReader{b: b[histogramChunkHeaderLen:]}
	h, err := readHistogramLayout(&r)
	if err != nil {
		return fmt.Errorf("%w: histogram chunk layout: %w", ErrCorrupt, err)
	}
	// The first histogram holds its timestamp, then its count, zero count,
	// sum and buckets as raw floats.
	pos, neg := countBuckets(h.PositiveSpans), countBuckets(h.NegativeSpans)
	if _, err := r.varbitInt(); err != nil || r.remaining() < 64*uint64(3+pos+neg) {
		return fmt.Errorf("%w: histogram chunk of %d bytes is too short for %d buckets", ErrCorrupt, len(b), pos+neg)
	}
	// Validate the layout as the layout of a histogram without observations.
	h.PositiveBuckets, h.NegativeBuckets = make([]float64, pos), make([]float64, neg)
	if err := model.ValidateHistogramLayout(h); err != nil {
		return fmt.Errorf("%w: histogram chunk layout: %w", ErrCorrupt, err)
	}
	return nil
}

// readHistogramLayout reads the layout of a float histogram chunk into a
// histogram without buckets. It reads at most model.MaxHistogramBuckets
// spans and custom bounds, and no more buckets than the rest of the chunk
// could hold as raw floats, and checks that the schema is valid, but leaves
// the other checks to model.ValidateHistogramLayout.
func readHistogramLayout(r *bitReader) (*histogram.FloatHistogram, error) {
	var h histogram.FloatHistogram
	maxBuckets := r.remaining() / 64
	var err error
	if h.ZeroThreshold, err = readZeroThreshold(r); err != nil {
		return nil, err
	}
	schema, err := r.varbitInt()
	if err != nil {
		return nil, err
	}
	h.Schema = int32(schema) // as chunkenc reads it
	if !histogram.IsValidSchema(h.Schema) {
		return nil, histogram.InvalidSchemaError(h.Schema)
	}
	spans := spanReader{r: r, maxBuckets: maxBuckets}
	if h.PositiveSpans, err = spans.read(); err != nil {
		return nil, err
	}
	if h.NegativeSpans, err = spans.read(); err != nil {
		return nil, err
	}
	if histogram.IsCustomBucketsSchema(h.Schema) {
		if h.CustomValues, err = readCustomBounds(r); err != nil {
			return nil, err
		}
	}
	return &h, nil
}

// readZeroThreshold reads the zero threshold of a layout: a byte that is 0
// for a threshold of 0, 255 if the threshold follows as a float64, and else
// encodes a threshold that is a power of two.
func readZeroThreshold(r *bitReader) (float64, error) {
	switch zt, err := r.readBits(8); {
	case err != nil:
		return 0, err
	case zt == 255:
		v, err := r.readBits(64)
		if err != nil {
			return 0, err
		}
		return math.Float64frombits(v), nil
	case zt != 0:
		return math.Ldexp(0.5, int(zt)-243), nil
	}
	return 0, nil
}

// spanReader reads the positive and then the negative spans of a layout.
// Its bounds hold for both sides together: at most model.MaxHistogramBuckets
// spans, with at most maxBuckets buckets.
type spanReader struct {
	r          *bitReader
	maxBuckets uint64
	spans      int // spans read so far
	buckets    int // buckets of the spans read so far
}

// read reads the spans of one side.
func (sr *spanReader) read() ([]histogram.Span, error) {
	n, err := sr.r.varbitUint()
	if err != nil {
		return nil, err
	}
	if n > uint64(model.MaxHistogramBuckets-sr.spans) {
		return nil, fmt.Errorf("more than %d spans", model.MaxHistogramBuckets)
	}
	sr.spans += int(n)
	var side []histogram.Span
	for range n {
		length, err := sr.r.varbitUint()
		if err != nil {
			return nil, err
		}
		offset, err := sr.r.varbitInt()
		if err != nil {
			return nil, err
		}
		if length > sr.maxBuckets-uint64(sr.buckets) {
			return nil, errors.New("more buckets than the chunk can hold")
		}
		sr.buckets += int(length)
		side = append(side, histogram.Span{Offset: int32(offset), Length: uint32(length)})
	}
	return side, nil
}

// readCustomBounds reads the custom bounds of a layout with the custom
// buckets schema, at most model.MaxHistogramBuckets of them.
func readCustomBounds(r *bitReader) ([]float64, error) {
	n, err := r.varbitUint()
	if err != nil {
		return nil, err
	}
	if n > model.MaxHistogramBuckets {
		return nil, fmt.Errorf("more than %d custom bounds", model.MaxHistogramBuckets)
	}
	var bounds []float64
	for range n {
		// A bound is stored as 1 + 1000 * bound if that is a whole
		// number that fits, else as 0 and the float64.
		v, err := r.varbitUint()
		if err != nil {
			return nil, err
		}
		bound := float64(v-1) / 1000
		if v == 0 {
			bits, err := r.readBits(64)
			if err != nil {
				return nil, err
			}
			bound = math.Float64frombits(bits)
		}
		bounds = append(bounds, bound)
	}
	return bounds, nil
}

func countBuckets(spans []histogram.Span) int {
	n := 0
	for _, s := range spans {
		n += int(s.Length)
	}
	return n
}

// sharedLayout gives the histograms decoded from one chunk a single copy of
// its spans. They all have the layout of the chunk, and chunkenc would
// otherwise copy it for each of them: a chunk of 16,384 histograms with
// thousands of spans, a few hundred kilobytes, would decode into gigabytes.
// The custom bounds are already shared: chunkenc interns them per chunk.
//
// The spans must therefore not be modified. The PromQL engine overwrites
// the histograms it is given, so the querier hands it copies (see
// copyingIterator in internal/query).
type sharedLayout struct {
	pos, neg []histogram.Span
}

// histogram returns a copy of fh, which the iterator will overwrite, with
// the spans of the chunk.
func (l *sharedLayout) histogram(fh *histogram.FloatHistogram) *histogram.FloatHistogram {
	if !slices.Equal(l.pos, fh.PositiveSpans) {
		l.pos = clone(fh.PositiveSpans)
	}
	if !slices.Equal(l.neg, fh.NegativeSpans) {
		l.neg = clone(fh.NegativeSpans)
	}
	return &histogram.FloatHistogram{
		CounterResetHint: fh.CounterResetHint,
		Schema:           fh.Schema,
		ZeroThreshold:    fh.ZeroThreshold,
		ZeroCount:        fh.ZeroCount,
		Count:            fh.Count,
		Sum:              fh.Sum,
		PositiveSpans:    l.pos,
		NegativeSpans:    l.neg,
		PositiveBuckets:  clone(fh.PositiveBuckets),
		NegativeBuckets:  clone(fh.NegativeBuckets),
		CustomValues:     fh.CustomValues,
	}
}

func clone[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return slices.Clone(s)
}

// bitReader reads a bit stream written by tsdb/chunkenc, most significant
// bit first.
type bitReader struct {
	b   []byte
	pos uint64 // bits read
}

func (r *bitReader) remaining() uint64 { return uint64(len(r.b))*8 - r.pos }

// readBits reads n <= 64 bits.
func (r *bitReader) readBits(n int) (uint64, error) {
	if uint64(n) > r.remaining() {
		return 0, io.ErrUnexpectedEOF
	}
	var v uint64
	for n > 0 {
		off := int(r.pos % 8)
		take := min(n, 8-off)
		v = v<<take | uint64(r.b[r.pos/8]>>(8-off-take))&(1<<take-1)
		r.pos += uint64(take)
		n -= take
	}
	return v, nil
}

// varbitWidths maps the number of 1 bits that start a varbit number to the
// width of the value that follows.
var varbitWidths = [9]int{0, 3, 6, 9, 12, 18, 25, 56, 64}

// varbitWidth reads the prefix of a varbit number: up to eight 1 bits,
// ended by a 0 bit unless there are eight.
func (r *bitReader) varbitWidth() (int, error) {
	ones := 0
	for ones < 8 {
		bit, err := r.readBits(1)
		if err != nil {
			return 0, err
		}
		if bit == 0 {
			break
		}
		ones++
	}
	return varbitWidths[ones], nil
}

func (r *bitReader) varbitUint() (uint64, error) {
	w, err := r.varbitWidth()
	if err != nil {
		return 0, err
	}
	return r.readBits(w)
}

func (r *bitReader) varbitInt() (int64, error) {
	w, err := r.varbitWidth()
	if err != nil {
		return 0, err
	}
	v, err := r.readBits(w)
	if err != nil {
		return 0, err
	}
	// Values of fewer than 64 bits are in [-2^(w-1)+1, 2^(w-1)].
	if w > 0 && w < 64 && v > 1<<(w-1) {
		v -= 1 << w
	}
	return int64(v), nil
}
