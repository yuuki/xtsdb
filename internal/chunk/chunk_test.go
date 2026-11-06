package chunk

import (
	"errors"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
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
