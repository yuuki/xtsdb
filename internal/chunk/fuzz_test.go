package chunk

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/value"

	"github.com/yuuki/xtsdb/internal/model"
)

// fuzzSamples reads float samples from b, 16 bytes each: a timestamp and the
// bits of a value, as in a raw record.
func fuzzSamples(b []byte) []model.Sample {
	var out []model.Sample
	for ; len(b) >= RecordLen && len(out) < 4096; b = b[RecordLen:] {
		out = append(out, model.Sample{
			T: int64(binary.BigEndian.Uint64(b)),
			V: math.Float64frombits(binary.BigEndian.Uint64(b[8:])),
		})
	}
	return out
}

func sameSamples(a, b []model.Sample) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].T != b[i].T || math.Float64bits(a[i].V) != math.Float64bits(b[i].V) || a[i].H != nil || b[i].H != nil {
			return false
		}
	}
	return true
}

// Float samples survive every encoding of both tiers bit for bit: raw
// records, compacted parts and cold-tier blobs. A chunk whose oldest records
// were compacted decodes to the same samples as the uncompacted chunk, the
// latest write of a timestamp winning in both, with the same record count.
func FuzzFloatRoundTrip(f *testing.F) {
	seed := func(samples ...model.Sample) []byte { return AppendRaw(nil, samples) }
	f.Add(seed(), uint16(0))
	f.Add(seed(model.Sample{T: 1, V: 1}, model.Sample{T: 2, V: 2}, model.Sample{T: 1, V: 3}), uint16(1))
	f.Add(seed(model.Sample{T: math.MinInt64, V: math.Inf(-1)}, model.Sample{T: math.MaxInt64, V: math.NaN()},
		model.Sample{T: 0, V: math.Float64frombits(value.StaleNaN)}, model.Sample{T: -1, V: math.Copysign(0, -1)}), uint16(2))
	f.Add(seed(model.Sample{T: 1_700_000_000_000, V: math.Float64frombits(histogramMark | 3)}), uint16(1))
	header := []byte("1793881662314273")
	f.Fuzz(func(t *testing.T, b []byte, split uint16) {
		in := fuzzSamples(b)
		want := make([]model.Sample, len(in))
		for i, s := range in {
			want[i] = model.Sample{T: s.T, V: CanonicalValue(s.V)}
		}
		want = Normalize(want)

		raw := AppendRaw(append([]byte(nil), header...), in)
		_, got, records, err := DecodeHot(raw, nil)
		if err != nil || records != len(in) || !sameSamples(got, want) {
			t.Fatalf("raw records: %v, %d records for %d samples, equal %v", err, records, len(in), sameSamples(got, want))
		}

		enc, blob, err := Encode(want)
		if err != nil || enc != EncodingXOR {
			t.Fatalf("encode: %v %v", enc, err)
		}
		if got, err := Decode(enc, blob); err != nil || !sameSamples(got, want) {
			t.Fatalf("cold blob: %v, equal %v", err, sameSamples(got, want))
		}

		// Compaction folds the oldest records into the compacted part.
		k := int(split) % (len(in) + 1)
		folded := make([]model.Sample, k)
		copy(folded, in[:k])
		for i := range folded {
			folded[i].V = CanonicalValue(folded[i].V)
		}
		compacted, err := EncodeCompacted(header, k, Normalize(folded))
		if err != nil {
			t.Fatal(err)
		}
		_, got, records, err = DecodeHot(AppendRaw(append([]byte(nil), header...), in[k:]), compacted)
		if err != nil || records != len(in) || !sameSamples(got, want) {
			t.Fatalf("compacted at %d: %v, %d records for %d samples, equal %v", k, err, records, len(in), sameSamples(got, want))
		}
	})
}
