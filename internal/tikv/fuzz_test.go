//go:build !notikv && (!darwin || cgo)

package tikv

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Series entries are read back from TiKV, which other clients can write.
// Decoding never panics, and what decodes encodes back to the same entry.
func FuzzSeriesEntry(f *testing.F) {
	f.Add(encodeSeries(labels.FromStrings("__name__", "up", "job", "a\x00b"), 1_700_000_000_000))
	f.Add(encodeSeries(labels.EmptyLabels(), -5))
	f.Add([]byte{formatVersion, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Add([]byte{formatVersion, 0, 0, 0, 0, 0, 0, 0, 0, 1, 'a'})
	f.Add([]byte{9, 9})
	f.Fuzz(func(t *testing.T, v []byte) {
		lset, expires, err := decodeSeries(v)
		if err != nil {
			return
		}
		again, exp2, err := decodeSeries(encodeSeries(lset, expires))
		if err != nil || !labels.Equal(again, lset) || exp2 != expires {
			t.Fatalf("round trip of %v (expires %d): %v %d %v", lset, expires, again, exp2, err)
		}
	})
}

// Chunk segments may be corrupt, missing, or left over from another write.
// Whatever one segment holds, Read neither fails nor panics, returns sorted
// samples, and returns only values that one of the versions of the window
// holds at that timestamp.
func FuzzCorruptSegment(f *testing.F) {
	f.Add(uint8(0), []byte(nil))
	f.Add(uint8(1), []byte{formatVersion})
	f.Add(uint8(2), appendSegment(nil, segmentHeader{encoding: chunk.EncodingXOR, segments: 1, maxT: 1, count: 1}, []byte{0, 1}))
	f.Add(uint8(3), appendSegment(nil, segmentHeader{encoding: chunk.EncodingTyped, segments: 3, maxT: 1 << 62, count: 1 << 31}, nil))
	f.Add(uint8(4), make([]byte, chunkHeaderLen+segmentSize+1))

	const chunkRange = int64(time.Hour / time.Millisecond)
	start := int64(20_000) * model.DayMillis
	rng := rand.New(rand.NewPCG(1, 2))
	// Version a: a part of incompressible values, in several segments, and a
	// small part. Version b, a later incarnation, overwrites every third
	// timestamp.
	var a, b []model.Sample
	for i := range 1600 {
		a = append(a, model.Sample{T: start + int64(i), V: rng.NormFloat64()})
		if i%3 == 0 {
			b = append(b, model.Sample{T: start + int64(i), V: float64(1000 + i)})
		}
	}
	want := make(map[int64][]float64)
	for _, s := range append(append([]model.Sample(nil), a...), b...) {
		want[s.T] = append(want[s.T], s.V)
	}
	parts := []struct {
		fragment int64
		part     int
		samples  []model.Sample
	}{{1, 0, a[:1500]}, {1, 1, a[1500:]}, {2, 0, b}}

	f.Fuzz(func(t *testing.T, which uint8, value []byte) {
		db, kv := newMemDB(t)
		store := NewChunkStore(db, 0, chunkRange, 2, nil)
		ctx := context.Background()
		for _, p := range parts {
			enc, data, err := chunk.Encode(p.samples)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.WriteChunk(ctx, storage.ColdChunk{Series: 1, Start: start, Fragment: p.fragment, Total: len(p.samples),
				Part: p.part, MaxT: p.samples[len(p.samples)-1].T, Count: len(p.samples), Encoding: enc, Data: data}); err != nil {
				t.Fatal(err)
			}
		}
		keys := kv.Keys(db.keys.chunkSeries(1))
		if len(keys) < 4 {
			t.Fatalf("%d segments, want the large part split", len(keys))
		}
		key := keys[int(which)%len(keys)]
		if len(value) == 0 {
			kv.Delete(key)
		} else if err := kv.PutWithTTL(ctx, key, value, 0); err != nil {
			t.Fatal(err)
		}
		got, err := store.Read(ctx, []model.SeriesID{1}, start, start+chunkRange)
		if err != nil {
			t.Fatalf("read with a corrupt segment: %v", err)
		}
		for i, s := range got[1] {
			if i > 0 && s.T <= got[1][i-1].T {
				t.Fatalf("samples out of order at %d", s.T)
			}
			ok := false
			for _, v := range want[s.T] {
				ok = ok || v == s.V
			}
			if !ok || s.H != nil {
				t.Fatalf("read %v, which no version holds (%v)", s, want[s.T])
			}
		}
	})
}
