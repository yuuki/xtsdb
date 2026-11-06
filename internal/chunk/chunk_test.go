package chunk

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/yuuki/xtsdb/internal/model"
)

func TestDecodeHotNormalizes(t *testing.T) {
	b := []byte("1793881662314273")
	b = AppendRaw(b, []model.Sample{{T: 3000, V: 3}, {T: 1000, V: 1}, {T: 2000, V: 2}, {T: 1000, V: 1.5}})
	frag, got, records, err := DecodeHot(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if frag != 1793881662314273 || records != 4 {
		t.Fatalf("fragment = %d, records = %d", frag, records)
	}
	want := []model.Sample{{T: 1000, V: 1.5}, {T: 2000, V: 2}, {T: 3000, V: 3}}
	assertSamples(t, got, want)
}

func TestDecodeHotRejectsCorruptInput(t *testing.T) {
	for _, b := range [][]byte{nil, []byte("123"), append([]byte("1793881662314273"), 1, 2, 3)} {
		if _, _, _, err := DecodeHot(b, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("len %d: expected ErrCorrupt, got %v", len(b), err)
		}
	}
}

func TestDecodeHotMergesCompactedPart(t *testing.T) {
	header := []byte("1793881662314273")
	// Compaction folded five records (one overwritten) into the compacted
	// part; the raw part holds what was appended since.
	folded := Normalize(rawRoundTrip(t, []model.Sample{
		{T: 1000, V: 1}, {T: 2000, V: 2}, {T: 3000, V: 3}, {T: 2000, V: 2.5}, {T: 5000, V: 5},
	}))
	compacted, err := EncodeCompacted(header, 5, folded)
	if err != nil {
		t.Fatal(err)
	}
	raw := AppendRaw(append([]byte{}, header...), []model.Sample{
		{T: 3000, V: 30}, {T: 4000, V: 4}, {T: 3000, V: 31}, {T: 5000, H: testHistogram(1, false)},
	})
	frag, got, records, err := DecodeHot(raw, compacted)
	if err != nil {
		t.Fatal(err)
	}
	if frag != 1793881662314273 || records != 9 {
		t.Fatalf("fragment = %d, records = %d", frag, records)
	}
	// Raw records were written after the compacted samples and win.
	assertSamples(t, got, []model.Sample{
		{T: 1000, V: 1}, {T: 2000, V: 2.5}, {T: 3000, V: 31}, {T: 4000, V: 4}, {T: 5000, H: testHistogram(1, false)},
	})

	// Without raw records.
	if _, got, records, err := DecodeHot(header, compacted); err != nil || records != 5 || len(got) != 4 {
		t.Fatalf("compacted only: %d samples, %d records, %v", len(got), records, err)
	}
	// A compacted part of another incarnation is corruption.
	other := append([]byte("1793881662314274"), compacted[HeaderLen:]...)
	if _, _, _, err := DecodeHot(raw, other); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for another incarnation, got %v", err)
	}
}

// rawRoundTrip round-trips samples through raw records, as compaction
// reads them.
func rawRoundTrip(t *testing.T, samples []model.Sample) []model.Sample {
	t.Helper()
	out, err := DecodeRaw(AppendRaw(nil, samples))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCompactedRejectsCorruptInput(t *testing.T) {
	header := []byte("1793881662314273")
	good, err := EncodeCompacted(header, 3, []model.Sample{{T: 1, V: 1}, {T: 2, V: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if s, n, err := DecodeCompacted(good); err != nil || n != 3 || len(s) != 2 {
		t.Fatalf("decode: %v %d %v", s, n, err)
	}
	for name, b := range map[string][]byte{
		"empty":        {},
		"short":        header[:10],
		"bad header":   append([]byte("17938816623142xx"), good[HeaderLen:]...),
		"no encoding":  good[:HeaderLen+1],
		"bad encoding": append(append([]byte{}, good[:HeaderLen+1]...), 9, 0),
		"truncated":    good[:len(good)-3],
		"few records":  append(append(append([]byte{}, header...), 1), good[HeaderLen+1:]...),
		"huge records": append(append(append([]byte{}, header...), 0xff, 0xff, 0xff, 0xff, 0x0f), good[HeaderLen+1:]...),
		"bad uvarint":  append(append([]byte{}, header...), 0xff),
	} {
		if _, _, err := DecodeCompacted(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: expected ErrCorrupt, got %v", name, err)
		}
	}
	// Samples out of order: compaction writes them sorted, so this is
	// corruption.
	unsorted, err := EncodeXOR([]model.Sample{{T: 2, V: 2}, {T: 1, V: 1}})
	if err != nil {
		t.Fatal(err)
	}
	b := append(append(append([]byte{}, header...), 2, byte(EncodingXOR)), unsorted...)
	if _, _, err := DecodeCompacted(b); !errors.Is(err, ErrCorrupt) {
		t.Errorf("out of order: expected ErrCorrupt, got %v", err)
	}
	if _, err := EncodeCompacted(header, 1, []model.Sample{{T: 1}, {T: 2}}); err == nil {
		t.Error("fewer records than samples must be refused")
	}
	if _, err := EncodeCompacted(header[:4], 1, nil); err == nil {
		t.Error("a short header must be refused")
	}
	if _, err := EncodeCompacted(header, 1, []model.Sample{{T: 1, H: testHistogram(1, false)}}); err == nil {
		t.Error("native histograms must not be compacted")
	}
	// Only XOR-encoded floats: a typed blob is corruption.
	_, typed, err := Encode([]model.Sample{{T: 1, H: testHistogram(1, false)}})
	if err != nil {
		t.Fatal(err)
	}
	b = append(append(append([]byte{}, header...), 1, byte(EncodingTyped)), typed...)
	if _, _, err := DecodeCompacted(b); !errors.Is(err, ErrCorrupt) {
		t.Errorf("typed encoding: expected ErrCorrupt, got %v", err)
	}
	// More samples than records claimed.
	many, err := EncodeXOR([]model.Sample{{T: 1}, {T: 2}, {T: 3}})
	if err != nil {
		t.Fatal(err)
	}
	b = append(append(append([]byte{}, header...), 2, byte(EncodingXOR)), many...)
	if _, _, err := DecodeCompacted(b); !errors.Is(err, ErrCorrupt) {
		t.Errorf("more samples than records: expected ErrCorrupt, got %v", err)
	}
}

func TestXORRoundTrip(t *testing.T) {
	var in []model.Sample
	for i := range 40000 { // spans several XOR segments
		in = append(in, model.Sample{T: 1_700_000_000_000 + int64(i)*15_000, V: float64(i%97) * 0.5})
	}
	in[10].V = math.NaN()
	in[11].V = math.Inf(1)
	b, err := EncodeXOR(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) >= len(in)*RecordLen/4 {
		t.Fatalf("poor compression: %d bytes for %d samples", len(b), len(in))
	}
	out, err := Decode(EncodingXOR, b)
	if err != nil {
		t.Fatal(err)
	}
	assertSamples(t, out, in)
}

func TestXOREmpty(t *testing.T) {
	b, err := EncodeXOR(nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeXOR(b)
	if err != nil || len(out) != 0 {
		t.Fatalf("got %v, %v", out, err)
	}
	if _, err := DecodeXOR([]byte{0xff}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestMergePrefersLaterSets(t *testing.T) {
	cold := []model.Sample{{T: 1, V: 1}, {T: 2, V: 2}, {T: 3, V: 3}}
	hot := []model.Sample{{T: 3, V: 30}, {T: 4, V: 40}}
	got := Merge(cold, nil, hot)
	assertSamples(t, got, []model.Sample{{T: 1, V: 1}, {T: 2, V: 2}, {T: 3, V: 30}, {T: 4, V: 40}})
	assertSamples(t, Merge(nil, hot), hot)
	if Merge() != nil {
		t.Fatal("empty merge must be nil")
	}
}

func TestFilter(t *testing.T) {
	s := []model.Sample{{T: 10}, {T: 20}, {T: 30}, {T: 40}}
	cases := []struct {
		mint, maxt int64
		want       int
	}{
		{0, 100, 4}, {20, 30, 2}, {21, 29, 0}, {40, 40, 1}, {41, 50, 0}, {0, 9, 0},
	}
	for _, c := range cases {
		if got := Filter(s, c.mint, c.maxt); len(got) != c.want {
			t.Errorf("Filter(%d,%d) = %d samples, want %d", c.mint, c.maxt, len(got), c.want)
		}
	}
}

func assertSamples(t *testing.T, got, want []model.Sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].T != want[i].T || math.Float64bits(got[i].V) != math.Float64bits(want[i].V) ||
			(got[i].H == nil) != (want[i].H == nil) ||
			// Re-encoding a chunk can add empty buckets; compare without them.
			(got[i].H != nil && !got[i].H.Copy().Compact(0).Equals(want[i].H.Copy().Compact(0))) {
			t.Fatalf("sample %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// testHistogram returns an exponential histogram, with custom buckets when
// custom is set.
func testHistogram(i int, custom bool) *histogram.FloatHistogram {
	h := &histogram.FloatHistogram{
		Schema:          1,
		ZeroThreshold:   0.001,
		ZeroCount:       float64(i),
		Count:           float64(10 + 3*i),
		Sum:             float64(i) * 1.5,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []float64{float64(5 + i), float64(5 + 2*i)},
	}
	if i%7 == 3 { // a bucket appears: the chunk is re-encoded
		h.PositiveSpans = []histogram.Span{{Offset: 0, Length: 3}}
		h.PositiveBuckets = append(h.PositiveBuckets, 0)
	}
	if custom {
		h.Schema, h.ZeroThreshold, h.ZeroCount = histogram.CustomBucketsSchema, 0, 0
		h.Count = h.PositiveBuckets[0] + h.PositiveBuckets[1]
		if len(h.PositiveBuckets) == 3 {
			h.CustomValues = []float64{0.1, 1, 10}
		} else {
			h.CustomValues = []float64{0.1, 1}
		}
	}
	return h
}

func TestHotHistogramRecords(t *testing.T) {
	in := []model.Sample{
		{T: 1000, V: 1},
		{T: 2000, H: testHistogram(1, false)},
		{T: 3000, H: testHistogram(2, true)},
		{T: 4000, V: math.Float64frombits(0x7ff4_0000_0000_0010)}, // looks like a histogram mark
		{T: 2000, H: testHistogram(3, false)},                     // overwrites
		{T: 5000, V: math.Float64frombits(0x7ff0000000000002)},    // stale marker is kept
	}
	b := AppendRaw([]byte("0000000000000001"), in)
	if (len(b)-HeaderLen)%RecordLen != 0 {
		t.Fatalf("records must keep chunks a multiple of %d bytes, got %d", RecordLen, len(b)-HeaderLen)
	}
	_, got, records, err := DecodeHot(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if records != len(in) {
		t.Fatalf("records = %d, want %d", records, len(in))
	}
	want := []model.Sample{in[0], in[4], in[2], {T: 4000, V: math.NaN()}, in[5]}
	if !math.IsNaN(got[3].V) || got[3].IsHistogram() {
		t.Fatalf("a NaN with the histogram mark must be stored as a plain NaN, got %+v", got[3])
	}
	got[3].V = want[3].V
	assertSamples(t, got, want)
	// A truncated histogram record is corrupt, not a crash.
	if _, _, _, err := DecodeHot(b[:HeaderLen+2*RecordLen], nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestTypedRoundTrip(t *testing.T) {
	var in []model.Sample
	for i := range 40000 {
		s := model.Sample{T: 1_700_000_000_000 + int64(i)*15_000}
		switch {
		case i < 100 || (i >= 20000 && i < 20100):
			s.V = float64(i) // floats between histograms
		case i%5000 == 17:
			s.H = testHistogram(0, false) // counter reset
		default:
			s.H = testHistogram(i, i >= 30000)
		}
		in = append(in, s)
	}
	enc, b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	if enc != EncodingTyped {
		t.Fatalf("encoding %d, want typed", enc)
	}
	out, err := Decode(enc, b)
	if err != nil {
		t.Fatal(err)
	}
	assertSamples(t, out, in)
	// Floats alone keep the XOR encoding.
	if enc, _, err := Encode(in[:100]); err != nil || enc != EncodingXOR {
		t.Fatalf("float-only encoding %d %v", enc, err)
	}
	if _, err := Decode(EncodingTyped, []byte{byte(chunkenc.EncHistogram), 1, 0}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

// Invalid histograms in a hot chunk are corruption: they must be reported,
// so that the chunk is quarantined, instead of making the cold encoder
// panic in the flusher.
func TestDecodeHotRejectsInvalidHistograms(t *testing.T) {
	bad := []*histogram.FloatHistogram{
		{Schema: 0, Count: 1, PositiveSpans: []histogram.Span{{Length: 2}}, PositiveBuckets: []float64{1}},
		{Schema: 100, Count: 1},
		{Schema: histogram.CustomBucketsSchema, Count: 2, PositiveSpans: []histogram.Span{{Length: 2}}, PositiveBuckets: []float64{1, 1}}, // no bounds
	}
	for i, h := range bad {
		b := AppendRaw([]byte("0000000000000001"), []model.Sample{{T: 1000, V: 1}, {T: 2000, H: h}})
		if _, _, _, err := DecodeHot(b, nil); !errors.Is(err, ErrCorrupt) {
			t.Errorf("case %d: expected ErrCorrupt, got %v", i, err)
		}
		// Even if one got through, encoding must fail instead of panicking.
		if _, _, err := Encode([]model.Sample{{T: 1000, H: testHistogram(1, false)}, {T: 2000, H: h}}); err == nil && i == 0 {
			t.Errorf("case %d: encoding an invalid histogram must fail", i)
		}
	}
	// A header that parses as a number but is not 16 digits.
	if _, _, _, err := DecodeHot([]byte("+000000000000001"), nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for a signed header, got %v", err)
	}
}

// The largest histogram accepted fits comfortably in one cold-tier row
// (4 MiB) and in a hot chunk.
func TestLargestHistogramFits(t *testing.T) {
	h := &histogram.FloatHistogram{Schema: 0, PositiveSpans: []histogram.Span{{Length: model.MaxHistogramBuckets}},
		PositiveBuckets: make([]float64, model.MaxHistogramBuckets)}
	for i := range h.PositiveBuckets {
		h.PositiveBuckets[i] = float64(i*7919%1000) + 0.5 // poorly compressible
		h.Count += h.PositiveBuckets[i]
	}
	if err := model.ValidateHistogram(h); err != nil {
		t.Fatal(err)
	}
	_, b, err := Encode([]model.Sample{{T: 1, H: h}})
	if err != nil || len(b) > 1<<20 {
		t.Fatalf("encoded %d bytes (%v), want at most 1 MiB", len(b), err)
	}
	if raw := AppendRaw(nil, []model.Sample{{T: 1, H: h}}); len(raw) > 1<<20 {
		t.Fatalf("hot record of %d bytes", len(raw))
	}
}

// crashTyped is a typed blob found by fuzzing DecodeHot: one float histogram
// chunk whose layout claims spans of billions of buckets, which made the
// chunkenc iterator allocate tens of gigabytes.
const crashTyped = "\x037\x01\x00\xff?PbM\xd2\xf1\xa9\xfc\x8cdE\x00\xa8\x00$$$$$$\x00\x00\x00\x00\x00\xff\xc0\x00p\x00\x00\x00\x00\xff\xe0\x00\x00\x00\x00\x00\x01\x00`\x00\x00\x00\x00\x00\x01\x00\x00\x00"

// Corrupt blobs in the persistent tier must be reported as ErrCorrupt: never
// panic, and never allocate much more memory than valid data of the same size.
func FuzzDecodeTyped(f *testing.F) {
	_, typed, err := Encode([]model.Sample{
		{T: 1, V: 1}, {T: 2, H: testHistogram(1, false)}, {T: 3, H: testHistogram(2, false)}, {T: 4, H: testHistogram(3, true)},
	})
	if err != nil {
		f.Fatal(err)
	}
	gauge := &histogram.FloatHistogram{CounterResetHint: histogram.GaugeType, Schema: -2, ZeroThreshold: 0.001, Count: 3,
		NegativeSpans: []histogram.Span{{Offset: -3, Length: 1}, {Offset: 2, Length: 2}}, NegativeBuckets: []float64{1, 1, 1}}
	_, other, err := Encode([]model.Sample{
		{T: 1, H: gauge}, {T: 2, H: &histogram.FloatHistogram{Sum: math.Float64frombits(value.StaleNaN)}},
		{T: 3, H: testHistogram(4, true)}, {T: 4, H: testHistogram(5, true)},
	})
	if err != nil {
		f.Fatal(err)
	}
	for _, b := range [][]byte{
		nil, typed, other, []byte(crashTyped),
		{byte(chunkenc.EncXOR), 1, 0},            // XOR chunk of 1 byte
		{byte(chunkenc.EncFloatHistogram), 1, 0}, // histogram chunk of 1 byte
		{byte(chunkenc.EncFloatHistogram), 3, 0, 1, 0},
		typed[:len(typed)-1],
	} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) { checkDecode(t, EncodingTyped, b) })
}

func FuzzDecodeXOR(f *testing.F) {
	xor, err := EncodeXOR([]model.Sample{{T: 1, V: 1}, {T: 2, V: 2}, {T: 1000, V: math.NaN()}})
	if err != nil {
		f.Fatal(err)
	}
	for _, b := range [][]byte{nil, xor, xor[:len(xor)-1], {0}, {1, 0}, {2, 0, 1}, {0xff}} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) { checkDecode(t, EncodingXOR, b) })
}

func checkDecode(t *testing.T, enc Encoding, b []byte) {
	samples, err := Decode(enc, b)
	if err != nil {
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("error is not ErrCorrupt: %v", err)
		}
		return
	}
	if len(samples) > MaxBlobSamples {
		t.Fatalf("%d samples, more than a blob holds", len(samples))
	}
}

// typedBlob builds a typed blob from chunks, bypassing the checks of Encode.
func typedBlob(chunks ...chunkenc.Chunk) []byte {
	var b []byte
	for _, c := range chunks {
		b = append(b, byte(c.Encoding()))
		b = binary.AppendUvarint(b, uint64(len(c.Bytes())))
		b = append(b, c.Bytes()...)
	}
	return b
}

// rawHistogramChunk encodes histograms into one chunk with chunkenc, which
// does not validate them.
func rawHistogramChunk(t *testing.T, hs ...*histogram.FloatHistogram) chunkenc.Chunk {
	t.Helper()
	c := chunkenc.NewFloatHistogramChunk()
	app, err := c.Appender()
	if err != nil {
		t.Fatal(err)
	}
	for i, h := range hs {
		next, _, a, err := app.AppendFloatHistogram(nil, 0, int64(i+1)*1000, h, true)
		if err != nil || next != nil {
			t.Fatalf("histogram %d: %v (new chunk %t)", i, err, next != nil)
		}
		app = a
	}
	return c
}

// zeroSpans returns n spans without buckets.
func zeroSpans(n int) []histogram.Span {
	s := make([]histogram.Span, n)
	for i := range s {
		s[i].Offset = 1
	}
	return s
}

func TestDecodeRejectsCorruptHistogramLayouts(t *testing.T) {
	tooMany := model.MaxHistogramBuckets + 1
	bounds := make([]float64, tooMany)
	for i := range bounds {
		bounds[i] = float64(i)
	}
	for name, h := range map[string]*histogram.FloatHistogram{
		"NaN zero threshold":      {Schema: 0, ZeroThreshold: math.NaN()},
		"negative zero threshold": {Schema: 0, ZeroThreshold: -1},
		"spans":                   {Schema: 0, PositiveSpans: zeroSpans(tooMany)},
		"reserved schema":         {Schema: 9, Count: 1, PositiveSpans: []histogram.Span{{Length: 1}}, PositiveBuckets: []float64{1}},
		"unknown schema":          {Schema: -20},
		"custom bounds": {Schema: histogram.CustomBucketsSchema, Count: 1,
			PositiveSpans: []histogram.Span{{Length: 1}}, PositiveBuckets: []float64{1}, CustomValues: bounds},
		"unsorted custom bounds": {Schema: histogram.CustomBucketsSchema, Count: 1,
			PositiveSpans: []histogram.Span{{Length: 1}}, PositiveBuckets: []float64{1}, CustomValues: []float64{2, 1}},
		"negative span offset": {Schema: 0, Count: 2, PositiveSpans: []histogram.Span{{Length: 1}, {Offset: -3, Length: 1}},
			PositiveBuckets: []float64{1, 1}},
	} {
		b := typedBlob(rawHistogramChunk(t, h))
		if _, err := Decode(EncodingTyped, b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: expected ErrCorrupt, got %v", name, err)
		}
	}

	if _, err := Decode(EncodingTyped, []byte(crashTyped)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("spans of billions of buckets: expected ErrCorrupt, got %v", err)
	}
	// A chunk too short for the buckets its layout claims.
	h := &histogram.FloatHistogram{Schema: 0, PositiveSpans: []histogram.Span{{Length: 1000}}, PositiveBuckets: make([]float64, 1000)}
	c := rawHistogramChunk(t, h).Bytes()
	if _, err := Decode(EncodingTyped, typedBlob(chunkFromData(t, chunkenc.EncFloatHistogram, c[:100]))); !errors.Is(err, ErrCorrupt) {
		t.Errorf("truncated chunk: expected ErrCorrupt, got %v", err)
	}
	// More samples than the encoder puts in a chunk.
	c = slices.Clone(rawHistogramChunk(t, testHistogram(1, false)).Bytes())
	binary.BigEndian.PutUint16(c, maxSamplesPerChunk+1)
	if _, err := Decode(EncodingTyped, typedBlob(chunkFromData(t, chunkenc.EncFloatHistogram, c))); !errors.Is(err, ErrCorrupt) {
		t.Errorf("too many samples: expected ErrCorrupt, got %v", err)
	}
}

func chunkFromData(t *testing.T, enc chunkenc.Encoding, b []byte) chunkenc.Chunk {
	t.Helper()
	c, err := chunkenc.FromData(enc, b)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// alternating returns 4 histograms of n buckets (in spans of one bucket if
// sparse) whose layouts alternate between two disjoint ranges, so that the
// layout of a chunk of all of them, the union of theirs, has 2n buckets.
func alternating(n int, sparse, gauge bool) []model.Sample {
	var out []model.Sample
	for i := range 4 {
		h := &histogram.FloatHistogram{Schema: 0, PositiveBuckets: make([]float64, n)}
		if gauge {
			h.CounterResetHint = histogram.GaugeType
		}
		first := int32(i%2) * 1_000_000
		if sparse {
			h.PositiveSpans = zeroSpans(n)
			for j := range h.PositiveSpans {
				h.PositiveSpans[j].Length = 1
			}
			h.PositiveSpans[0].Offset = first
		} else {
			h.PositiveSpans = []histogram.Span{{Offset: first, Length: uint32(n)}}
		}
		for j := range h.PositiveBuckets {
			// Counters keep the empty buckets of earlier histograms, so
			// only the first one is empty.
			if gauge || i > 0 {
				h.PositiveBuckets[j] = float64(1 + i)
			}
			h.Count += h.PositiveBuckets[j]
		}
		out = append(out, model.Sample{T: int64(i+1) * 1000, H: h})
	}
	return out
}

// countChunks returns the number of chunks in a typed blob.
func countChunks(t *testing.T, b []byte) int {
	t.Helper()
	n := 0
	for len(b) > 0 {
		size, sz := binary.Uvarint(b[1:])
		if sz <= 0 {
			t.Fatal("bad blob")
		}
		b = b[1+sz+int(size):]
		n++
	}
	return n
}

// The layout of a chunk is the union of the layouts of its histograms. It can
// have more buckets than one histogram may, which decoding accepts, but the
// encoder must cut chunks rather than write a layout that decoding refuses,
// such as one of more than model.MaxHistogramBuckets spans.
func TestEncodeCutsChunksAtLayoutLimit(t *testing.T) {
	const n = 10_000 // buckets or spans of each histogram; two layouts make 20,000
	for _, c := range []struct {
		sparse, gauge bool
		chunks        int
	}{
		// Counters cut at the counter resets of the last two histograms,
		// so their first chunk holds the union of two layouts.
		{false, false, 3}, {false, true, 1}, // a chunk of 20,000 buckets
		{true, false, 4}, {true, true, 4}, // 20,000 spans would be refused: a chunk per histogram
	} {
		in := alternating(n, c.sparse, c.gauge)
		orig := make([]*histogram.FloatHistogram, len(in))
		for i, s := range in {
			orig[i] = s.H.Copy()
		}
		enc, b, err := Encode(in)
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		if n := countChunks(t, b); n != c.chunks {
			t.Errorf("%+v: %d chunks, want %d", c, n, c.chunks)
		}
		out, err := Decode(enc, b)
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		assertSamples(t, out, in)
		for i, s := range in {
			if !s.H.Equals(orig[i]) {
				t.Fatalf("%+v: Encode modified histogram %d", c, i)
			}
		}
	}
}

// Chunks written by earlier builds, which did not cut chunks at any layout
// size, stay readable when their layout has more buckets than one histogram
// may.
func TestDecodeAcceptsLayoutsOfEarlierBuilds(t *testing.T) {
	for _, gauge := range []bool{false, true} {
		in := alternating(10_000, false, gauge)
		if !gauge {
			in = in[:2] // a counter reset follows
		}
		c := chunkenc.Chunk(chunkenc.NewFloatHistogramChunk())
		app, err := c.Appender()
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range in {
			next, recoded, a, err := app.AppendFloatHistogram(nil, 0, s.T, s.H.Copy(), false)
			if err != nil || (next != nil && !recoded) {
				t.Fatalf("gauge %t: %v (new chunk %t)", gauge, err, next != nil)
			}
			if next != nil {
				c = next
			}
			app = a
		}
		out, err := Decode(EncodingTyped, typedBlob(c))
		if err != nil {
			t.Fatalf("gauge %t: %v", gauge, err)
		}
		assertSamples(t, out, in)
	}
}

// Histograms decoded from a chunk share its spans and custom bounds: one
// copy per histogram could take gigabytes for a chunk of a few hundred
// kilobytes.
func TestDecodeSharesLayout(t *testing.T) {
	var in []model.Sample
	for i := range 1000 {
		in = append(in, model.Sample{T: int64(i+1) * 1000, H: &histogram.FloatHistogram{
			Schema: histogram.CustomBucketsSchema, Count: float64(i),
			PositiveSpans:   append(zeroSpans(model.MaxHistogramBuckets-1), histogram.Span{Length: 1}),
			PositiveBuckets: []float64{float64(i)},
			CustomValues:    customBounds(model.MaxHistogramBuckets),
		}})
	}
	enc, b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out, err := Decode(enc, b)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	assertSamples(t, out, in)
	// Spans and bounds take 256 KiB per histogram if they are not shared.
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 16<<20 {
		t.Errorf("decoding %d bytes allocated %d MiB", len(b), alloc>>20)
	}
	h0, h1 := out[0].H, out[1].H
	if &h0.PositiveSpans[0] != &h1.PositiveSpans[0] || &h0.CustomValues[0] != &h1.CustomValues[0] {
		t.Error("histograms of a chunk must share its layout")
	}
	if &h0.PositiveBuckets[0] == &h1.PositiveBuckets[0] {
		t.Error("histograms must not share buckets")
	}
}

func customBounds(n int) []float64 {
	b := make([]float64, n)
	for i := range b {
		b[i] = float64(i+1) / 10
	}
	return b
}

// The encoder must write layouts that decoding accepts, whatever the
// histograms: compare random sequences of counter and gauge histograms
// after a round trip.
func TestHistogramRoundTripRandomLayouts(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	randomHistogram := func(schema int32, gauge bool) *histogram.FloatHistogram {
		h := &histogram.FloatHistogram{Schema: schema, ZeroThreshold: 0.001}
		if gauge {
			h.CounterResetHint = histogram.GaugeType
		}
		custom := schema == histogram.CustomBucketsSchema
		if custom {
			h.ZeroThreshold, h.CustomValues = 0, []float64{0.1, 1, 10, 100, 1000}
		} else {
			h.ZeroCount = float64(rng.IntN(5))
		}
		sides := []*[]histogram.Span{&h.PositiveSpans, &h.NegativeSpans}
		buckets := []*[]float64{&h.PositiveBuckets, &h.NegativeBuckets}
		for side := range 2 {
			if custom && side == 1 {
				break
			}
			covered := 0
			for i := range rng.IntN(4) {
				offset := rng.IntN(3)
				if i == 0 && !custom {
					offset = rng.IntN(20) - 10
				}
				length := rng.IntN(4)
				if custom && covered+offset+length > len(h.CustomValues)+1 {
					break
				}
				covered += offset + length
				*sides[side] = append(*sides[side], histogram.Span{Offset: int32(offset), Length: uint32(length)})
				for range length {
					v := float64(rng.IntN(10))
					*buckets[side] = append(*buckets[side], v)
					h.Count += v
				}
			}
		}
		h.Count += h.ZeroCount
		h.Sum = rng.Float64()
		return h
	}
	var in []model.Sample
	for i := range 3000 {
		if i%500 == 0 { // the schema changes
			in = append(in, model.Sample{T: int64(i) * 1000, V: float64(i)})
			continue
		}
		schema := int32(i/500%3) - 1
		if i/500 == 4 {
			schema = histogram.CustomBucketsSchema
		}
		in = append(in, model.Sample{T: int64(i) * 1000, H: randomHistogram(schema, i/250%2 == 1)})
	}
	for i, s := range in {
		if s.H != nil {
			if err := model.ValidateHistogram(s.H); err != nil {
				t.Fatalf("sample %d: %v", i, err)
			}
		}
	}
	enc, b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(enc, b)
	if err != nil {
		t.Fatal(err)
	}
	assertSamples(t, out, in)
}

func TestDecodeLimitsSamples(t *testing.T) {
	in := make([]model.Sample, MaxBlobSamples+1)
	for i := range in {
		in[i] = model.Sample{T: int64(i), V: 1}
	}
	b, err := EncodeXOR(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(EncodingXOR, b); !errors.Is(err, ErrCorrupt) {
		t.Errorf("a blob of more than %d samples: expected ErrCorrupt, got %v", MaxBlobSamples, err)
	}
	if out, err := DecodeXOR(b); err != nil || len(out) != len(in) {
		t.Errorf("DecodeXOR: %d samples, %v", len(out), err)
	}
	b, err = EncodeXOR(in[:MaxBlobSamples])
	if err != nil {
		t.Fatal(err)
	}
	if out, err := Decode(EncodingXOR, b); err != nil || len(out) != MaxBlobSamples {
		t.Errorf("a blob of %d samples: %d samples, %v", MaxBlobSamples, len(out), err)
	}
	// XOR chunks of fewer than 2 bytes, or more samples than the encoder
	// writes.
	tooMany := binary.BigEndian.AppendUint16(nil, maxSamplesPerChunk+1)
	for _, seg := range [][]byte{{}, {0}, append(tooMany, make([]byte, 16)...)} {
		blob := append(binary.AppendUvarint(nil, uint64(len(seg))), seg...)
		if _, err := Decode(EncodingXOR, blob); !errors.Is(err, ErrCorrupt) {
			t.Errorf("XOR chunk %x: expected ErrCorrupt, got %v", seg, err)
		}
	}
}

// readHistogramLayout must read the layouts that chunkenc writes, in all
// their encodings.
func TestReadHistogramLayout(t *testing.T) {
	for i, h := range []*histogram.FloatHistogram{
		{Schema: 0}, // no buckets
		{Schema: -4, ZeroThreshold: math.Ldexp(1, -128), Count: 3, // zero threshold in one byte
			PositiveSpans:   []histogram.Span{{Offset: -1000, Length: 1}, {Offset: 70000, Length: 2}},
			PositiveBuckets: []float64{1, 1, 1}},
		{Schema: 8, ZeroThreshold: 0.001, ZeroCount: 1, Count: 2, // zero threshold as a float
			NegativeSpans: []histogram.Span{{Offset: 5, Length: 0}, {Offset: 0, Length: 1}}, NegativeBuckets: []float64{1}},
		{Schema: 4, Count: 6, // the largest values of each varbit width
			PositiveSpans: []histogram.Span{{Offset: 4, Length: 1}, {Offset: 32, Length: 1}, {Offset: 256, Length: 1},
				{Offset: 2048, Length: 1}, {Offset: 131072, Length: 1}, {Offset: 16777216, Length: 1}},
			PositiveBuckets: slices.Repeat([]float64{1}, 6)},
		{Schema: 3, ZeroThreshold: 1e-300, Count: 300,
			PositiveSpans: []histogram.Span{{Offset: math.MaxInt32, Length: 300}}, PositiveBuckets: slices.Repeat([]float64{1}, 300)},
		{Schema: histogram.CustomBucketsSchema, Count: 2, // bounds as varbits and as floats
			PositiveSpans:   []histogram.Span{{Offset: 1, Length: 1}, {Offset: 2, Length: 1}},
			PositiveBuckets: []float64{1, 1}, CustomValues: []float64{-5, 0, 0.1, 33554.43, 33554.431, 1e9}},
	} {
		if err := model.ValidateHistogram(h); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		b := rawHistogramChunk(t, h).Bytes()
		if err := checkHistogramChunk(b); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		r := bitReader{b: b[histogramChunkHeaderLen:]}
		got, err := readHistogramLayout(&r)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		got.PositiveBuckets, got.NegativeBuckets = h.PositiveBuckets, h.NegativeBuckets
		got.Count, got.ZeroCount = h.Count, h.ZeroCount
		if !got.Equals(h) || !slices.Equal(got.PositiveSpans, h.PositiveSpans) || !slices.Equal(got.NegativeSpans, h.NegativeSpans) {
			t.Errorf("case %d: read %+v, want %+v", i, got, h)
		}
		// Next comes the timestamp of the first histogram.
		if ts, err := r.varbitInt(); err != nil || ts != 1000 {
			t.Errorf("case %d: timestamp %d, %v", i, ts, err)
		}
	}
}

// Corrupt chunks must be reported as ErrCorrupt, never panic: the flusher
// would crash on every chunk it claims after the corrupt one.
func FuzzDecodeHot(f *testing.F) {
	header := []byte("1793881662314273")
	good, err := EncodeCompacted(header, 2, []model.Sample{{T: 1, V: 1}, {T: 2, V: 2}})
	if err != nil {
		f.Fatal(err)
	}
	_, typed, err := Encode([]model.Sample{{T: 1, H: testHistogram(1, false)}})
	if err != nil {
		f.Fatal(err)
	}
	hist := append(append(append([]byte{}, header...), 1, byte(EncodingTyped)), typed...)
	raw := AppendRaw(append([]byte{}, header...), []model.Sample{{T: 3, V: 3}, {T: 4, H: testHistogram(2, false)}})
	for _, z := range [][]byte{
		nil, good, hist,
		append(append([]byte{}, header...), 1, byte(EncodingXOR), 1, 0),      // XOR segment of 1 byte
		append(append([]byte{}, header...), 1, byte(EncodingXOR), 0),         // XOR segment of 0 bytes
		append(append([]byte{}, header...), 1, byte(EncodingTyped), 7, 1, 0), // histogram segment of 1 byte
		good[:len(good)-1],
	} {
		f.Add(raw, z)
		f.Add(header, z)
	}
	f.Fuzz(func(t *testing.T, raw, compacted []byte) {
		_, samples, records, err := DecodeHot(raw, compacted)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error is not ErrCorrupt: %v", err)
			}
			return
		}
		if records < len(samples) {
			t.Fatalf("%d records for %d samples", records, len(samples))
		}
	})
}
