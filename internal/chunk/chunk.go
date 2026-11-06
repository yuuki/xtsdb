// Package chunk implements the sample encodings used by the two tiers.
//
// The hot tier stores a chunk as a fixed header followed by raw records
// appended in arrival order, which lets Redis append without reading. A
// float sample is one 16-byte record; a native histogram is a 16-byte record
// followed by its encoding, padded to a multiple of 16 bytes.
// The cold tier stores sorted, de-duplicated samples compressed with the
// Gorilla XOR encoding from the Prometheus TSDB, and native histograms with
// its float histogram encoding.
package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/prometheus/prometheus/model/histogram"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/yuuki/xtsdb/internal/model"
)

// HeaderLen is the length of the hot chunk header: the fragment of the
// chunk, rendered by the Redis server as 16 ASCII digits. Fragments come
// from a hybrid clock (microseconds, kept increasing per shard) and order
// the incarnations of a window.
const HeaderLen = 16

// RecordLen is the size of one raw float record, and the unit in which hot
// chunks grow.
const RecordLen = 16

// ErrCorrupt is returned for undecodable chunk data.
var ErrCorrupt = errors.New("corrupt chunk")

// histogramMark marks a histogram record in the value field of a raw record.
// The value field then holds histogramMark | payload length, which is a NaN
// that float records never contain (see CanonicalValue).
const (
	histogramMark     = 0x7ff4_0000_0000_0000
	histogramMarkMask = 0xffff_0000_0000_0000
	maxPayload        = 0xffff_ffff
)

// CanonicalValue returns v, except that NaNs whose bits could be mistaken
// for a histogram record become the canonical NaN. PromQL does not tell
// NaNs apart, except the stale marker, which is kept.
func CanonicalValue(v float64) float64 {
	if math.Float64bits(v)&histogramMarkMask == histogramMark {
		return math.NaN()
	}
	return v
}

// AppendRaw appends samples in the hot-tier record format to dst. A float
// sample takes 16 bytes: its timestamp and value. A histogram takes 16 bytes
// (its timestamp, and the length of its encoding marked as a histogram) plus
// its encoding, padded to a multiple of 16 bytes.
func AppendRaw(dst []byte, samples []model.Sample) []byte {
	for _, s := range samples {
		dst = binary.BigEndian.AppendUint64(dst, uint64(s.T))
		if s.H == nil {
			dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(CanonicalValue(s.V)))
			continue
		}
		h := writev2.FromFloatHistogram(0, s.T, s.H)
		h.Timestamp = 0 // already in the record
		payload, err := h.Marshal()
		if err != nil || len(payload) > maxPayload {
			// Marshalling a valid histogram cannot fail; never write a
			// record that cannot be read back.
			panic(fmt.Sprintf("encode histogram: %v (%d bytes)", err, len(payload)))
		}
		dst = binary.BigEndian.AppendUint64(dst, histogramMark|uint64(len(payload)))
		dst = append(dst, payload...)
		if pad := len(payload) % RecordLen; pad != 0 {
			dst = append(dst, make([]byte, RecordLen-pad)...)
		}
	}
	return dst
}

// DecodeHot decodes a hot chunk value. It returns the fragment, the samples
// sorted by time with duplicates resolved in favour of the latest write, and
// the number of records the chunk holds (including overwritten samples).
func DecodeHot(b []byte) (fragment int64, samples []model.Sample, records int, err error) {
	if len(b) < HeaderLen || (len(b)-HeaderLen)%RecordLen != 0 {
		return 0, nil, 0, fmt.Errorf("%w: length %d", ErrCorrupt, len(b))
	}
	// Only 16 ASCII digits: the fragment is re-rendered that way to fence
	// completion, so any other spelling could never be completed.
	for _, c := range b[:HeaderLen] {
		if c < '0' || c > '9' {
			return 0, nil, 0, fmt.Errorf("%w: header %q", ErrCorrupt, b[:HeaderLen])
		}
	}
	fragment, err = strconv.ParseInt(string(b[:HeaderLen]), 10, 64)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("%w: header: %w", ErrCorrupt, err)
	}
	samples, err = DecodeRaw(b[HeaderLen:])
	if err != nil {
		return 0, nil, 0, err
	}
	records = len(samples)
	return fragment, Normalize(samples), records, nil
}

// DecodeRaw decodes raw records without sorting them.
func DecodeRaw(b []byte) ([]model.Sample, error) {
	if len(b)%RecordLen != 0 {
		return nil, fmt.Errorf("%w: length %d", ErrCorrupt, len(b))
	}
	out := make([]model.Sample, 0, len(b)/RecordLen)
	for len(b) > 0 {
		t := int64(binary.BigEndian.Uint64(b[0:8]))
		word := binary.BigEndian.Uint64(b[8:16])
		b = b[RecordLen:]
		if word&histogramMarkMask != histogramMark {
			out = append(out, model.Sample{T: t, V: math.Float64frombits(word)})
			continue
		}
		n := int(word &^ histogramMarkMask)
		padded := (n + RecordLen - 1) / RecordLen * RecordLen
		if n > maxPayload || padded > len(b) {
			return nil, fmt.Errorf("%w: histogram record of %d bytes exceeds the chunk", ErrCorrupt, n)
		}
		var h writev2.Histogram
		if err := h.Unmarshal(b[:n]); err != nil {
			return nil, fmt.Errorf("%w: histogram: %w", ErrCorrupt, err)
		}
		fh := h.ToFloatHistogram()
		// Only valid histograms are written; anything else is corruption,
		// and would make the encoders of the cold tier panic.
		if err := model.ValidateHistogram(fh); err != nil {
			return nil, fmt.Errorf("%w: histogram at %d: %w", ErrCorrupt, t, err)
		}
		out = append(out, model.Sample{T: t, H: fh})
		b = b[padded:]
	}
	return out, nil
}

// Normalize sorts samples by time in place and removes duplicate timestamps,
// keeping the sample that appeared last in the input.
func Normalize(samples []model.Sample) []model.Sample {
	slices.SortStableFunc(samples, func(a, b model.Sample) int {
		switch {
		case a.T < b.T:
			return -1
		case a.T > b.T:
			return 1
		}
		return 0
	})
	out := samples[:0]
	for i, s := range samples {
		if i+1 < len(samples) && samples[i+1].T == s.T {
			continue // a later write for the same timestamp wins
		}
		out = append(out, s)
	}
	return out
}

// Merge combines sample sets in priority order: for equal timestamps the
// sample from the later set wins. Each set must be sorted.
func Merge(sets ...[]model.Sample) []model.Sample {
	n := 0
	nonEmpty := 0
	for _, s := range sets {
		n += len(s)
		if len(s) > 0 {
			nonEmpty++
		}
	}
	if nonEmpty <= 1 {
		for _, s := range sets {
			if len(s) > 0 {
				return s
			}
		}
		return nil
	}
	all := make([]model.Sample, 0, n)
	for _, s := range sets {
		all = append(all, s...)
	}
	return Normalize(all)
}

// Filter returns the samples within [mint, maxt]. samples must be sorted.
func Filter(samples []model.Sample, mint, maxt int64) []model.Sample {
	lo, _ := slices.BinarySearchFunc(samples, mint, func(s model.Sample, t int64) int {
		return cmpInt64(s.T, t)
	})
	hi, found := slices.BinarySearchFunc(samples, maxt, func(s model.Sample, t int64) int {
		return cmpInt64(s.T, t)
	})
	if found {
		hi++
	}
	if lo >= hi {
		return nil
	}
	return samples[lo:hi]
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Encoding identifies the cold-tier blob format.
type Encoding uint8

const (
	// EncodingXOR is a sequence of length-prefixed Prometheus XOR chunks. It
	// is used for windows that hold only float samples.
	EncodingXOR Encoding = 1
	// EncodingTyped is a sequence of segments, each a Prometheus chunk
	// encoding byte (XOR or float histogram), the uvarint length of the
	// chunk and the chunk. It is used for windows with native histograms.
	EncodingTyped Encoding = 2
)

// maxSamplesPerChunk stays well below the 16-bit sample counter of a
// Prometheus chunk. Decoding refuses chunks with more samples.
const maxSamplesPerChunk = 16384

// Encode compresses sorted, de-duplicated samples.
//
// The Prometheus chunk encoders panic on some invalid histograms. Samples
// are validated when they are written, but a panic here would take down the
// flusher, and with it every flusher that claims the chunk after it, so it
// is returned as an error instead.
func Encode(samples []model.Sample) (enc Encoding, b []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			enc, b, err = 0, nil, fmt.Errorf("%w: encode: %v", ErrCorrupt, r)
		}
	}()
	if !slices.ContainsFunc(samples, model.Sample.IsHistogram) {
		b, err := EncodeXOR(samples)
		return EncodingXOR, b, err
	}
	var out []byte
	for i := 0; i < len(samples); {
		j := i + 1
		for j < len(samples) && samples[j].IsHistogram() == samples[i].IsHistogram() {
			j++
		}
		var (
			chunks []chunkenc.Chunk
			err    error
		)
		if samples[i].IsHistogram() {
			chunks, err = histogramChunks(samples[i:j])
		} else {
			chunks, err = xorChunks(samples[i:j])
		}
		if err != nil {
			return 0, nil, err
		}
		for _, c := range chunks {
			out = append(out, byte(c.Encoding()))
			out = binary.AppendUvarint(out, uint64(len(c.Bytes())))
			out = append(out, c.Bytes()...)
		}
		i = j
	}
	return EncodingTyped, out, nil
}

func xorChunks(samples []model.Sample) ([]chunkenc.Chunk, error) {
	var out []chunkenc.Chunk
	for start := 0; start < len(samples); start += maxSamplesPerChunk {
		c := chunkenc.NewXORChunk()
		app, err := c.Appender()
		if err != nil {
			return nil, err
		}
		for _, s := range samples[start:min(start+maxSamplesPerChunk, len(samples))] {
			app.Append(0, s.T, s.V)
		}
		out = append(out, c)
	}
	return out, nil
}

// histogramChunks encodes float histograms. The appender cuts a new chunk
// on a counter reset or an incompatible bucket layout, and re-encodes the
// current one when buckets are added. The layout of a chunk is then the
// union of the layouts of its histograms; when that would no longer be the
// layout of a valid histogram, which decoding requires (at most
// model.MaxHistogramBuckets buckets, for example), a new chunk is cut
// instead.
func histogramChunks(samples []model.Sample) ([]chunkenc.Chunk, error) {
	var (
		out []chunkenc.Chunk
		cur chunkenc.Chunk
		app chunkenc.Appender
	)
	cut := func() (err error) {
		if cur != nil {
			out = append(out, cur)
		}
		cur = chunkenc.NewFloatHistogramChunk()
		app, err = cur.Appender()
		return err
	}
	if err := cut(); err != nil {
		return nil, err
	}
	for _, s := range samples {
		if cur.NumSamples() >= maxSamplesPerChunk {
			if err := cut(); err != nil {
				return nil, err
			}
		}
		// The appender modifies the histograms it is given.
		next, recoded, nextApp, err := app.AppendFloatHistogram(nil, 0, s.T, s.H.Copy(), false)
		if err != nil {
			return nil, fmt.Errorf("encode histogram at %d: %w", s.T, err)
		}
		if recoded && checkHistogramChunk(next.Bytes()) != nil {
			if err := cut(); err != nil {
				return nil, err
			}
			if _, _, app, err = app.AppendFloatHistogram(nil, 0, s.T, s.H.Copy(), false); err != nil {
				return nil, fmt.Errorf("encode histogram at %d: %w", s.T, err)
			}
			continue
		}
		if next != nil {
			if !recoded {
				out = append(out, cur)
			}
			cur = next
		}
		app = nextApp
	}
	out = append(out, cur)
	// Never write what decoding refuses, even if an invalid histogram got
	// this far.
	for _, c := range out {
		if err := checkHistogramChunk(c.Bytes()); err != nil {
			return nil, fmt.Errorf("encode histograms: %w", err)
		}
	}
	return out, nil
}

// EncodeXOR compresses sorted, de-duplicated float samples.
func EncodeXOR(samples []model.Sample) ([]byte, error) {
	chunks, err := xorChunks(samples)
	if err != nil {
		return nil, err
	}
	var out []byte
	for _, c := range chunks {
		out = binary.AppendUvarint(out, uint64(len(c.Bytes())))
		out = append(out, c.Bytes()...)
	}
	return out, nil
}

// MaxBlobSamples is the largest number of samples in a cold-tier blob: a
// version of a window is stored in parts of at most this many samples (see
// flush.WriteCold), and Decode refuses blobs with more.
const MaxBlobSamples = 100_000

// DecodeXOR decompresses a blob produced by EncodeXOR.
func DecodeXOR(b []byte) ([]model.Sample, error) { return decodeXOR(b, -1) }

// decodeXOR decompresses a blob produced by EncodeXOR. It fails before it
// decodes more than limit samples, unless limit is negative.
func decodeXOR(b []byte, limit int) ([]model.Sample, error) {
	var out []model.Sample
	var it chunkenc.Iterator
	for len(b) > 0 {
		n, sz := binary.Uvarint(b)
		if sz <= 0 || uint64(len(b)-sz) < n {
			return nil, fmt.Errorf("%w: bad segment length", ErrCorrupt)
		}
		b = b[sz:]
		var err error
		if out, it, err = decodeChunk(out, it, chunkenc.EncXOR, b[:n], limit); err != nil {
			return nil, err
		}
		b = b[n:]
	}
	return out, nil
}

// decodeTyped decompresses a blob produced by Encode with EncodingTyped. It
// fails before it decodes more than limit samples, unless limit is negative.
func decodeTyped(b []byte, limit int) ([]model.Sample, error) {
	var out []model.Sample
	var it chunkenc.Iterator
	for len(b) > 0 {
		enc := chunkenc.Encoding(b[0])
		if enc != chunkenc.EncXOR && enc != chunkenc.EncFloatHistogram {
			return nil, fmt.Errorf("%w: unexpected chunk encoding %v", ErrCorrupt, enc)
		}
		n, sz := binary.Uvarint(b[1:])
		if sz <= 0 || uint64(len(b)-1-sz) < n {
			return nil, fmt.Errorf("%w: bad segment length", ErrCorrupt)
		}
		b = b[1+sz:]
		var err error
		if out, it, err = decodeChunk(out, it, enc, b[:n], limit); err != nil {
			return nil, err
		}
		b = b[n:]
	}
	return out, nil
}

// decodeChunk appends the samples of a Prometheus chunk to out, failing
// before out holds more than limit samples unless limit is negative.
//
// The chunkenc iterators trust their input. Chunks are therefore checked
// first against what the encoder writes, so that corrupt data cannot make
// decoding take much more memory than valid data of the same size (see
// checkHistogramChunk), and a panic on corrupt data is returned as
// ErrCorrupt. The iterator must not be reused after an error.
func decodeChunk(out []model.Sample, it chunkenc.Iterator, enc chunkenc.Encoding, b []byte, limit int) (
	res []model.Sample, iter chunkenc.Iterator, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, iter, err = nil, nil, fmt.Errorf("%w: decode: %v", ErrCorrupt, r)
		}
	}()
	// Both encodings start with the sample count, a big-endian uint16.
	if len(b) < 2 {
		return nil, it, fmt.Errorf("%w: chunk of %d bytes", ErrCorrupt, len(b))
	}
	switch n := int(binary.BigEndian.Uint16(b)); {
	case n > maxSamplesPerChunk:
		return nil, it, fmt.Errorf("%w: chunk of %d samples", ErrCorrupt, n)
	case limit >= 0 && len(out)+n > limit:
		return nil, it, fmt.Errorf("%w: more than %d samples", ErrCorrupt, limit)
	}
	if enc == chunkenc.EncFloatHistogram {
		if err := checkHistogramChunk(b); err != nil {
			return nil, it, err
		}
	}
	c, err := chunkenc.FromData(enc, b)
	if err != nil {
		return nil, it, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	var (
		fh     histogram.FloatHistogram // overwritten by every histogram
		layout sharedLayout
	)
	it = c.Iterator(it)
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		switch vt {
		case chunkenc.ValFloat:
			t, v := it.At()
			out = append(out, model.Sample{T: t, V: v})
		case chunkenc.ValFloatHistogram:
			t, h := it.AtFloatHistogram(&fh)
			if h == &fh { // stale markers come as a new histogram
				h = layout.histogram(h)
			}
			out = append(out, model.Sample{T: t, H: h})
		default:
			return nil, it, fmt.Errorf("%w: unexpected value type %v", ErrCorrupt, vt)
		}
	}
	if err := it.Err(); err != nil {
		return nil, it, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return out, it, nil
}

// Decode decodes a cold-tier blob of the given encoding. Corrupt data is
// reported as ErrCorrupt.
func Decode(enc Encoding, b []byte) ([]model.Sample, error) {
	switch enc {
	case EncodingXOR:
		return decodeXOR(b, MaxBlobSamples)
	case EncodingTyped:
		return decodeTyped(b, MaxBlobSamples)
	default:
		return nil, fmt.Errorf("%w: unknown encoding %d", ErrCorrupt, enc)
	}
}
