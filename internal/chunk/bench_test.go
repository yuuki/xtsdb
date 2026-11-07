package chunk

import (
	"math"
	"testing"

	"github.com/yuuki/xtsdb/internal/model"
)

// benchSamples returns an hour of a counter-like series at a 15 s interval,
// the default chunk range.
func benchSamples() []model.Sample {
	out := make([]model.Sample, 240)
	for i := range out {
		out[i] = model.Sample{T: 1_700_000_000_000 + int64(i)*15_000, V: math.Floor(float64(i*i) / 7)}
	}
	return out
}

// Encoding a closed window for the persistent tier, in the flusher.
func BenchmarkEncode(b *testing.B) {
	samples := benchSamples()
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := Encode(samples); err != nil {
			b.Fatal(err)
		}
	}
}

// Decoding a part read from the persistent tier, in the querier.
func BenchmarkDecode(b *testing.B) {
	enc, blob, err := Encode(benchSamples())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(blob)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Decode(enc, blob); err != nil {
			b.Fatal(err)
		}
	}
}

// Decoding a hot chunk whose first half was compacted, in the flusher and
// the querier.
func BenchmarkDecodeHot(b *testing.B) {
	samples := benchSamples()
	header := []byte("1793881662314273")
	compacted, err := EncodeCompacted(header, 120, samples[:120])
	if err != nil {
		b.Fatal(err)
	}
	raw := AppendRaw(append([]byte(nil), header...), samples[120:])
	b.SetBytes(int64(len(raw) + len(compacted)))
	b.ReportAllocs()
	for b.Loop() {
		if _, got, _, err := DecodeHot(raw, compacted); err != nil || len(got) != len(samples) {
			b.Fatal(err)
		}
	}
}
