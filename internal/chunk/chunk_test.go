package chunk

import (
	"errors"
	"math"
	"testing"

	"github.com/yuuki/xtsdb/internal/model"
)

func TestDecodeHotNormalizes(t *testing.T) {
	b := []byte("1793881662314273")
	b = AppendRaw(b, []model.Sample{{T: 3000, V: 3}, {T: 1000, V: 1}, {T: 2000, V: 2}, {T: 1000, V: 1.5}})
	frag, got, err := DecodeHot(b)
	if err != nil {
		t.Fatal(err)
	}
	if frag != 1793881662314273 {
		t.Fatalf("fragment = %d", frag)
	}
	want := []model.Sample{{T: 1000, V: 1.5}, {T: 2000, V: 2}, {T: 3000, V: 3}}
	assertSamples(t, got, want)
}

func TestDecodeHotRejectsCorruptInput(t *testing.T) {
	for _, b := range [][]byte{nil, []byte("123"), append([]byte("1793881662314273"), 1, 2, 3)} {
		if _, _, err := DecodeHot(b); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("len %d: expected ErrCorrupt, got %v", len(b), err)
		}
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
		if got[i].T != want[i].T || math.Float64bits(got[i].V) != math.Float64bits(want[i].V) {
			t.Fatalf("sample %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}
