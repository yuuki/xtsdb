// Package chunk implements the sample encodings used by the two tiers.
//
// The hot tier stores a chunk as a fixed header followed by raw 16-byte
// records appended in arrival order, which lets Redis append without reading.
// The cold tier stores sorted, de-duplicated samples compressed with the
// Gorilla XOR encoding from the Prometheus TSDB.
package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/yuuki/xtsdb/internal/model"
)

// HeaderLen is the length of the hot chunk header: the fragment of the
// chunk, rendered by the Redis server as 16 ASCII digits. Fragments come
// from a hybrid clock (microseconds, kept increasing per shard) and order
// the incarnations of a window.
const HeaderLen = 16

// RecordLen is the size of one raw sample record.
const RecordLen = 16

// ErrCorrupt is returned for undecodable chunk data.
var ErrCorrupt = errors.New("corrupt chunk")

// AppendRaw appends samples in the hot-tier record format to dst.
func AppendRaw(dst []byte, samples []model.Sample) []byte {
	for _, s := range samples {
		dst = binary.BigEndian.AppendUint64(dst, uint64(s.T))
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(s.V))
	}
	return dst
}

// DecodeHot decodes a hot chunk value. It returns the fragment and the
// samples sorted by time with duplicates resolved in favour of the latest
// write.
func DecodeHot(b []byte) (fragment int64, samples []model.Sample, err error) {
	if len(b) < HeaderLen || (len(b)-HeaderLen)%RecordLen != 0 {
		return 0, nil, fmt.Errorf("%w: length %d", ErrCorrupt, len(b))
	}
	fragment, err = strconv.ParseInt(string(b[:HeaderLen]), 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: header: %w", ErrCorrupt, err)
	}
	samples, err = DecodeRaw(b[HeaderLen:])
	if err != nil {
		return 0, nil, err
	}
	return fragment, Normalize(samples), nil
}

// DecodeRaw decodes raw records without sorting them.
func DecodeRaw(b []byte) ([]model.Sample, error) {
	if len(b)%RecordLen != 0 {
		return nil, fmt.Errorf("%w: length %d", ErrCorrupt, len(b))
	}
	out := make([]model.Sample, len(b)/RecordLen)
	for i := range out {
		r := b[i*RecordLen:]
		out[i] = model.Sample{
			T: int64(binary.BigEndian.Uint64(r[0:8])),
			V: math.Float64frombits(binary.BigEndian.Uint64(r[8:16])),
		}
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

// EncodingXOR is a sequence of length-prefixed Prometheus XOR chunks.
const EncodingXOR Encoding = 1

// maxSamplesPerXOR stays well below the 16-bit sample counter of an XOR chunk.
const maxSamplesPerXOR = 16384

// EncodeXOR compresses sorted, de-duplicated samples.
func EncodeXOR(samples []model.Sample) ([]byte, error) {
	var out []byte
	for start := 0; start < len(samples); start += maxSamplesPerXOR {
		end := min(start+maxSamplesPerXOR, len(samples))
		c := chunkenc.NewXORChunk()
		app, err := c.Appender()
		if err != nil {
			return nil, err
		}
		for _, s := range samples[start:end] {
			app.Append(0, s.T, s.V)
		}
		b := c.Bytes()
		out = binary.AppendUvarint(out, uint64(len(b)))
		out = append(out, b...)
	}
	return out, nil
}

// DecodeXOR decompresses a blob produced by EncodeXOR.
func DecodeXOR(b []byte) ([]model.Sample, error) {
	var out []model.Sample
	var it chunkenc.Iterator
	for len(b) > 0 {
		n, sz := binary.Uvarint(b)
		if sz <= 0 || uint64(len(b)-sz) < n {
			return nil, fmt.Errorf("%w: bad segment length", ErrCorrupt)
		}
		b = b[sz:]
		c, err := chunkenc.FromData(chunkenc.EncXOR, b[:n])
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		b = b[n:]
		it = c.Iterator(it)
		for it.Next() == chunkenc.ValFloat {
			t, v := it.At()
			out = append(out, model.Sample{T: t, V: v})
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
	}
	return out, nil
}

// Decode decodes a cold-tier blob of the given encoding.
func Decode(enc Encoding, b []byte) ([]model.Sample, error) {
	switch enc {
	case EncodingXOR:
		return DecodeXOR(b)
	default:
		return nil, fmt.Errorf("%w: unknown encoding %d", ErrCorrupt, enc)
	}
}
