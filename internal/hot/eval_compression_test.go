//go:build compressioneval

package hot

// Measurements behind docs/hot-compression.md: how much Gorilla compression
// saves in the hot tier, and what it costs. They are not regular tests: they
// need a scratch Redis server, whose data they DELETE (FLUSHALL), and print
// their results. Run them with
//
//	XTSDB_EVAL_REDIS=127.0.0.1:6399 go test -tags compressioneval -run Eval -v -timeout 60m ./internal/hot/

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// gen generates the values of one kind of series.
type gen struct {
	name string
	next func(r *rand.Rand, prev float64) float64
	init float64
}

// gens is a deliberately pessimistic mix: real Prometheus data compresses to
// about 1.3 to 2 bytes per sample, most of these kinds do worse.
var gens = []gen{
	{"const (up)", func(*rand.Rand, float64) float64 { return 1 }, 1},
	{"small int gauge", func(r *rand.Rand, p float64) float64 {
		if r.Float64() < 0.3 {
			return math.Max(0, p+float64(r.IntN(7)-3))
		}
		return p
	}, 40},
	{"int counter", func(r *rand.Rand, p float64) float64 { return p + float64(r.IntN(200)) }, 1e6},
	{"bytes counter", func(r *rand.Rand, p float64) float64 { return p + float64(r.IntN(10_000_000)) }, 1e12},
	{"cpu seconds (0.01)", func(r *rand.Rand, p float64) float64 {
		return math.Round((p+0.01*float64(r.IntN(1500)))*100) / 100
	}, 123456.78},
	{"mem bytes (4KiB)", func(r *rand.Rand, p float64) float64 { return p + 4096*float64(r.IntN(2001)-1000) }, 8e9},
	{"random float gauge", func(r *rand.Rand, _ float64) float64 { return r.Float64() }, 0.5},
}

// series returns n samples 15 seconds apart, optionally with ±2 ms jitter.
func series(g gen, n int, jitter bool, seed uint64) []model.Sample {
	r := rand.New(rand.NewPCG(seed, 1))
	t0 := int64(1_762_000_000_000)
	v := g.init
	out := make([]model.Sample, n)
	for i := range out {
		t := t0 + int64(i)*15_000
		if jitter {
			t += int64(r.IntN(5) - 2)
		}
		out[i] = model.Sample{T: t, V: v}
		v = g.next(r, v)
	}
	return out
}

// encodeBlocks encodes samples as independent XOR blocks of size block.
func encodeBlocks(t testing.TB, s []model.Sample, block int) []byte {
	var out []byte
	for i := 0; i < len(s); i += block {
		b, err := chunk.EncodeXOR(s[i:min(i+block, len(s))])
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b...)
	}
	return out
}

func evalClient(t testing.TB) redis.UniversalClient {
	addr := os.Getenv("XTSDB_EVAL_REDIS")
	if addr == "" {
		t.Skip("XTSDB_EVAL_REDIS not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func flushAll(t testing.TB, c redis.UniversalClient) {
	if err := c.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

func usedMemory(t testing.TB, c redis.UniversalClient) int64 {
	info, err := c.Info(context.Background(), "memory").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(info, "\r\n") {
		if v, ok := strings.CutPrefix(l, "used_memory:"); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	t.Fatal("no used_memory in INFO")
	return 0
}

// scriptMicros returns the Redis time spent in scripts and the number of
// script calls since the last CONFIG RESETSTAT.
func scriptMicros(t testing.TB, c redis.UniversalClient) (usec, calls float64) {
	info, err := c.Info(context.Background(), "commandstats").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(info, "\r\n") {
		if !strings.HasPrefix(l, "cmdstat_evalsha:") && !strings.HasPrefix(l, "cmdstat_eval:") {
			continue
		}
		for _, kv := range strings.Split(strings.SplitN(l, ":", 2)[1], ",") {
			if v, ok := strings.CutPrefix(kv, "usec="); ok {
				x, _ := strconv.ParseFloat(v, 64)
				usec += x
			}
			if v, ok := strings.CutPrefix(kv, "calls="); ok {
				x, _ := strconv.ParseFloat(v, 64)
				calls += x
			}
		}
	}
	return usec, calls
}

// build writes one value per series with build, in pipelines, and returns
// the growth of used_memory.
func build(t testing.TB, c redis.UniversalClient, n int, add func(p redis.Pipeliner, i int)) int64 {
	ctx := context.Background()
	flushAll(t, c)
	base := usedMemory(t, c)
	for lo := 0; lo < n; lo += 500 {
		p := c.Pipeline()
		for i := lo; i < min(lo+500, n); i++ {
			add(p, i)
		}
		if _, err := p.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return usedMemory(t, c) - base
}

var evalHeader = []byte("0001762000000000")

func appendEach(ctx context.Context, p redis.Pipeliner, key string, samples []model.Sample) {
	for _, s := range samples {
		p.Append(ctx, key, string(chunk.AppendRaw(nil, []model.Sample{s})))
	}
}

// TestEvalBlockCompression: bytes per sample of the Prometheus XOR encoding
// when samples are compressed in independent blocks. A block of 1 is what
// compressing each remote write batch on its own would achieve; 240 is a
// whole window of 1 hour at 15 s.
func TestEvalBlockCompression(t *testing.T) {
	blocks := []int{1, 2, 4, 8, 20, 40, 80, 240}
	for _, jitter := range []bool{false, true} {
		fmt.Printf("\n15s interval, jitter=%v, bytes/sample by block size (raw hot record = 16)\n%-20s", jitter, "series")
		for _, b := range blocks {
			fmt.Printf("%7d", b)
		}
		fmt.Println()
		mean := make([]float64, len(blocks))
		for gi, g := range gens {
			s := series(g, 240, jitter, uint64(gi+1))
			fmt.Printf("%-20s", g.name)
			for i, b := range blocks {
				v := float64(len(encodeBlocks(t, s, b))) / float64(len(s))
				mean[i] += v / float64(len(gens))
				fmt.Printf("%7.2f", v)
			}
			fmt.Println()
		}
		fmt.Printf("%-20s", "mean")
		for _, v := range mean {
			fmt.Printf("%7.2f", v)
		}
		fmt.Println()
	}
}

// TestEvalAppendSlack: Redis grows strings greedily on APPEND, so a chunk
// built by appends takes more memory than the same value written at once.
func TestEvalAppendSlack(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	flushAll(t, c)
	rec := string(make([]byte, chunk.RecordLen))
	fmt.Println()
	for _, n := range []int{20, 80, 160, 240} {
		_ = c.Set(ctx, "a", string(evalHeader), 0).Err()
		for range n {
			_ = c.Append(ctx, "a", rec).Err()
		}
		_ = c.Set(ctx, "s", strings.Repeat("x", chunk.HeaderLen+chunk.RecordLen*n), 0).Err()
		a, _ := c.MemoryUsage(ctx, "a", 0).Result()
		s, _ := c.MemoryUsage(ctx, "s", 0).Result()
		fmt.Printf("%3d records (%4d bytes): MEMORY USAGE after APPENDs %5d, after one SET %5d\n",
			n, chunk.HeaderLen+chunk.RecordLen*n, a, s)
	}
}

// TestEvalLayoutsMemory: memory of chunk keys for several hot layouts, with
// 160 samples per series (the average held with a 1h chunk_range, 10m grace
// and 15 s interval), compaction every 40 samples for the layouts with a
// raw tail.
func TestEvalLayoutsMemory(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	const nSeries, perSeries, block = 20000, 160, 40
	data := make([][]model.Sample, nSeries)
	for i := range data {
		data[i] = series(gens[i%len(gens)], perSeries, false, uint64(i+7))
	}
	n := float64(nSeries * perSeries)
	report := func(name string, d int64) {
		fmt.Printf("%-48s %6.2f B/sample %6.0f B/series\n", name, float64(d)/n, float64(d)/nSeries)
	}
	fmt.Printf("\nchunk keys, %d series x %d samples\n", nSeries, perSeries)
	report("A raw, APPEND per sample (current)", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		k := fmt.Sprintf("c:%d", i)
		p.Set(ctx, k, evalHeader, 0)
		appendEach(ctx, p, k, data[i])
	}))
	report("E raw, one SET", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		p.Set(ctx, fmt.Sprintf("c:%d", i), append(append([]byte{}, evalHeader...), chunk.AppendRaw(nil, data[i])...), 0)
	}))
	report("B Gorilla, whole window, one SET", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		p.Set(ctx, fmt.Sprintf("c:%d", i), append(append([]byte{}, evalHeader...), encodeBlocks(t, data[i], perSeries)...), 0)
	}))
	report("B' Gorilla, blocks of 40, one SET", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		p.Set(ctx, fmt.Sprintf("c:%d", i), append(append([]byte{}, evalHeader...), encodeBlocks(t, data[i], block)...), 0)
	}))
	report("C one key: SET blocks, then APPEND raw tail", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		d, r := data[i], i%block
		k := fmt.Sprintf("c:%d", i)
		p.Set(ctx, k, append(append([]byte{}, evalHeader...), encodeBlocks(t, d[:len(d)-r], block)...), 0)
		appendEach(ctx, p, k, d[len(d)-r:])
	}))
	report("D two keys: z = SET blocks, c = APPEND raw tail", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		d, r := data[i], i%block
		p.Set(ctx, fmt.Sprintf("z:%d", i), encodeBlocks(t, d[:len(d)-r], block), 0)
		k := fmt.Sprintf("c:%d", i)
		p.Set(ctx, k, evalHeader, 0)
		appendEach(ctx, p, k, d[len(d)-r:])
	}))

	// The real store, with a layout in which the 160 samples fit one window:
	// chunk keys plus due set and window indexes.
	flushAll(t, c)
	base := usedMemory(t, c)
	st, err := New(ctx, c, Options{Prefix: "ev", Lease: time.Minute,
		Layout: model.Layout{Shards: 256, ChunkRange: 24 * time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UnixMilli() - perSeries*15_000
	ids := make([]model.SeriesID, nSeries)
	for i := range ids {
		ids[i] = model.SeriesID(rand.Uint64())
	}
	start := time.Now()
	for k := range perSeries {
		batch := make([]storage.SeriesSamples, nSeries)
		for i, id := range ids {
			batch[i] = storage.SeriesSamples{ID: id, Samples: []model.Sample{{T: t0 + int64(k)*15000, V: data[i][k].V}}}
		}
		if res, err := st.Append(ctx, batch); err != nil || res.Accepted != nSeries {
			t.Fatalf("append: %+v %v", res, err)
		}
	}
	elapsed := time.Since(start)
	used := usedMemory(t, c) - base
	fmt.Printf("real store (current): used_memory %.2f B/sample, %.0f B/series, %.0f samples/s from one client\n",
		float64(used)/n, float64(used)/nSeries, n/elapsed.Seconds())
}

// TestEvalSteadyStateMemory: as TestEvalLayoutsMemory, but with chunks
// filled uniformly from 1 to 240 samples, as they are at any moment in a
// running system.
func TestEvalSteadyStateMemory(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	const nSeries = 24000
	data := make([][]model.Sample, nSeries)
	n := 0.0
	for i := range data {
		data[i] = series(gens[i%len(gens)], 1+i%240, false, uint64(i+11))
		n += float64(len(data[i]))
	}
	report := func(name string, d int64) { fmt.Printf("%-50s %6.2f B/sample\n", name, float64(d)/n) }
	twoKeys := func(every int) func(p redis.Pipeliner, i int) {
		return func(p redis.Pipeliner, i int) {
			d := data[i]
			r := len(d) % every
			if len(d) > r {
				p.Set(ctx, fmt.Sprintf("z:%d", i), encodeBlocks(t, d[:len(d)-r], len(d)), 0)
			}
			k := fmt.Sprintf("c:%d", i)
			p.Set(ctx, k, evalHeader, 0)
			appendEach(ctx, p, k, d[len(d)-r:])
		}
	}
	fmt.Printf("\nsteady state: chunk keys, fill uniform in 1..240 samples (mean %.1f)\n", n/nSeries)
	report("A raw, APPEND per sample (current)", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		k := fmt.Sprintf("c:%d", i)
		p.Set(ctx, k, evalHeader, 0)
		appendEach(ctx, p, k, data[i])
	}))
	report("E raw, one SET", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		p.Set(ctx, fmt.Sprintf("c:%d", i), append(append([]byte{}, evalHeader...), chunk.AppendRaw(nil, data[i])...), 0)
	}))
	report("B Gorilla, whole chunk, one SET (lower bound)", build(t, c, nSeries, func(p redis.Pipeliner, i int) {
		p.Set(ctx, fmt.Sprintf("c:%d", i), append(append([]byte{}, evalHeader...), encodeBlocks(t, data[i], len(data[i]))...), 0)
	}))
	report("D two keys, compaction every 40 samples", build(t, c, nSeries, twoKeys(40)))
	report("D two keys, compaction every 20 samples", build(t, c, nSeries, twoKeys(20)))
}

// luaGorillaModel performs the arithmetic and the Redis calls of a Gorilla
// encoder written in Lua, with the bits built as strings. It is an early
// cost model (it writes placeholder bytes); the BITFIELD prototype in
// eval_bitfield_test.go is a complete encoder.
var luaGorillaModel = redis.NewScript(`
local function clz32(x) if x == 0 then return 32 end local n = 0
  while bit.band(x, 0x80000000) == 0 do x = bit.lshift(x, 1) n = n + 1 end return n end
local function ctz32(x) if x == 0 then return 32 end local n = 0
  while bit.band(x, 1) == 0 do x = bit.rshift(x, 1) n = n + 1 end return n end
local written = 0
for c = 1, #KEYS do
  local key, data = KEYS[c], ARGV[c]
  local st = redis.call('GETRANGE', key, 0, 31)
  local pt, pd, phi, plo, plead, ptrail, pbits = 0, 0, 0, 0, 0, 0, 0
  if #st == 32 then
    pt, pd, phi, plo, plead, ptrail, pbits = struct.unpack('>dd I4I4 I1I1 I4', st)
  end
  local bits = {}
  for off = 1, #data, 16 do
    local hi, lo = struct.unpack('>I4I4', data, off)
    local t = hi * 4294967296 + lo
    local vhi, vlo = struct.unpack('>I4I4', data, off + 8)
    local d = t - pt
    local dod = d - pd
    if dod == 0 then bits[#bits + 1] = '0'
    elseif dod >= -8191 and dod <= 8192 then bits[#bits + 1] = string.format('10%014d', 0)
    else bits[#bits + 1] = string.format('1110%020d', 0) end
    local xhi, xlo = bit.bxor(vhi, phi), bit.bxor(vlo, plo)
    if xhi == 0 and xlo == 0 then bits[#bits + 1] = '0'
    else
      local lead = (xhi ~= 0) and clz32(xhi) or 32 + clz32(xlo)
      local trail = (xlo ~= 0) and ctz32(xlo) or 32 + ctz32(xhi)
      if lead >= plead and trail >= ptrail then bits[#bits + 1] = '10' .. string.rep('1', 64 - plead - ptrail)
      else bits[#bits + 1] = '11' .. string.rep('1', 11 + 64 - lead - trail) plead, ptrail = lead, trail end
    end
    pt, pd, phi, plo = t, d, vhi, vlo
  end
  local s = table.concat(bits)
  pbits = pbits + #s
  local nbytes = math.floor(#s / 8)
  redis.call('SETRANGE', key, 0, struct.pack('>dd I4I4 I1I1 I4', pt, pd, phi, plo, plead, ptrail, pbits))
  redis.call('APPEND', key, string.rep('x', nbytes))
  written = written + nbytes
end
return written
`)

// TestEvalLuaGorillaModelCost: Redis time per sample of the current append
// script and of the Lua cost model, one random float sample per chunk and
// 256 chunks per call (the remote write pattern).
func TestEvalLuaGorillaModelCost(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	flushAll(t, c)
	const nChunks, rounds = 256, 400
	r := rand.New(rand.NewPCG(3, 4))
	now := time.Now().UnixMilli()
	run := func(name string, call func(k int) error) {
		_ = c.ConfigResetStat(ctx).Err()
		for k := range rounds {
			if err := call(k); err != nil {
				t.Fatal(err)
			}
		}
		usec, calls := scriptMicros(t, c)
		fmt.Printf("%-28s %5.2f us/sample\n", name, usec/(calls*nChunks))
	}
	st, err := New(ctx, c, Options{Prefix: "lc", Lease: time.Minute,
		Layout: model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: 10 * time.Minute.Milliseconds()}})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]model.SeriesID, nChunks)
	for i := range ids {
		ids[i] = model.SeriesID(r.Uint64())
	}
	fmt.Println()
	run("current append (raw)", func(k int) error {
		batch := make([]storage.SeriesSamples, nChunks)
		for i, id := range ids {
			batch[i] = storage.SeriesSamples{ID: id, Samples: []model.Sample{{T: now - int64(rounds-k)*1000, V: r.Float64()}}}
		}
		_, err := st.Append(ctx, batch)
		return err
	})
	keys := make([]string, nChunks)
	for i := range keys {
		keys[i] = fmt.Sprintf("g:{0}:c:%d", i)
	}
	run("Lua Gorilla cost model", func(k int) error {
		args := make([]any, nChunks)
		for i := range args {
			args[i] = chunk.AppendRaw(nil, []model.Sample{{T: now - int64(rounds-k)*15000, V: r.Float64()}})
		}
		return luaGorillaModel.Run(ctx, c, keys, args...).Err()
	})
}

// compactModel is the compare-and-swap of a compaction in the two-key
// layout (c = header + raw tail, z = compressed), before it was implemented.
// KEYS[1] = due set, KEYS[2] = in-flight set, then (c, z) per chunk.
// ARGV per chunk: member, due, header, c length read, z length read, new z.
var compactModel = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local done = 0
for c = 0, (#KEYS - 2) / 2 - 1 do
  local ck, zk = KEYS[3 + 2 * c], KEYS[4 + 2 * c]
  local j = 1 + 6 * c
  local member, due, hdr, clen, zlen, newz = ARGV[j], tonumber(ARGV[j+1]), ARGV[j+2], tonumber(ARGV[j+3]), tonumber(ARGV[j+4]), ARGV[j+5]
  if now < due and not redis.call('ZSCORE', KEYS[2], member)
     and redis.call('GETRANGE', ck, 0, 15) == hdr
     and redis.call('STRLEN', zk) == zlen and redis.call('STRLEN', ck) >= clen then
    local tail = redis.call('GETRANGE', ck, clen, -1)
    redis.call('SET', ck, hdr .. tail)
    redis.call('SET', zk, newz)
    done = done + 1
  end
end
return done
`)

// TestEvalCompactionModelCost: Redis time of the compaction CAS, per chunk
// and per sample, for chunks of 160 samples of which 40 are raw.
func TestEvalCompactionModelCost(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	flushAll(t, c)
	const nChunks, rounds, tail = 256, 200, 40
	due := time.Now().Add(time.Hour).UnixMilli()
	_ = c.ConfigResetStat(ctx).Err()
	var zBytes int
	for k := range rounds {
		p := c.Pipeline()
		data := make([][]model.Sample, nChunks)
		for i := range nChunks {
			data[i] = series(gens[i%len(gens)], 160, false, uint64(k*nChunks+i))
			p.Set(ctx, fmt.Sprintf("x:{0}:c:%d", i), string(evalHeader)+string(chunk.AppendRaw(nil, data[i][160-tail:])), 0)
			p.Set(ctx, fmt.Sprintf("x:{0}:z:%d", i), "", 0)
		}
		if _, err := p.Exec(ctx); err != nil {
			t.Fatal(err)
		}
		keys := []string{"x:{0}:due", "x:{0}:inf"}
		var args []any
		for i := range nChunks {
			z, _ := chunk.EncodeXOR(data[i])
			zBytes += len(z)
			keys = append(keys, fmt.Sprintf("x:{0}:c:%d", i), fmt.Sprintf("x:{0}:z:%d", i))
			args = append(args, fmt.Sprintf("m%d", i), due, string(evalHeader), chunk.HeaderLen+tail*chunk.RecordLen, 0, z)
		}
		if n, err := compactModel.Run(ctx, c, keys, args...).Int(); err != nil || n != nChunks {
			t.Fatal(n, err)
		}
	}
	usec, calls := scriptMicros(t, c)
	perChunk := usec / (calls * nChunks)
	fmt.Printf("\ncompaction CAS: %.2f us per chunk, %.2f us per raw sample folded (%d raw samples, %d B new z)\n",
		perChunk, perChunk/tail, tail, zBytes/(rounds*nChunks))
}

// swapAll compacts every open chunk with the compaction script, without the
// rules of Store.Compact (minimum of records, no growth), to measure what a
// compaction does to memory.
func swapAll(t testing.TB, st *Store, layout model.Layout) {
	ctx := context.Background()
	for shard := range layout.Shards {
		members, err := st.client.ZRange(ctx, st.dueKey(shard), 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range members {
			cmd := st.client.MGet(ctx, st.chunkKey(shard, m), st.compactedKey(shard, m))
			raw, compacted, ok, err := chunkParts(cmd)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			_, samples, records, err := chunk.DecodeHot(raw, compacted)
			if err != nil {
				t.Fatal(err)
			}
			next, err := chunk.EncodeCompacted(raw[:chunk.HeaderLen], records, samples)
			if err != nil {
				t.Fatal(err)
			}
			n, err := compactScript.Run(ctx, st.client, []string{st.dueKey(shard), st.chunkKey(shard, m), st.compactedKey(shard, m)},
				m, raw, compacted, next).Int64Slice()
			if err != nil || len(n) != 1 {
				t.Fatal(n, err)
			}
		}
	}
}

// TestEvalCompactionMemoryDelta: change of used_memory per chunk when a
// chunk is compacted with n raw records appended one at a time, the first
// time (which creates its compacted part) and after a first compaction of
// 40 records.
func TestEvalCompactionMemoryDelta(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	const nChunks = 2000
	layout := model.Layout{Shards: 16, ChunkRange: 24 * time.Hour.Milliseconds(), Grace: 72 * time.Hour.Milliseconds()}
	counts := []int{1, 2, 4, 6, 8, 12, 16, 24, 32}
	for _, prior := range []int{0, 40} {
		fmt.Printf("\ncompaction after %d compacted records: change of used_memory per chunk (bytes) by raw records\n%-20s", prior, "series")
		for _, n := range counts {
			fmt.Printf("%7d", n)
		}
		fmt.Println()
		for gi, g := range gens {
			fmt.Printf("%-20s", g.name)
			for _, n := range counts {
				flushAll(t, c)
				st, err := New(ctx, c, Options{Prefix: "fc", Layout: layout, Lease: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UnixMilli()
				data := make([][]model.Sample, nChunks)
				for i := range data {
					data[i] = series(g, prior+n, false, uint64(gi*nChunks+i))
				}
				appendRounds := func(from, to int) {
					for k := from; k < to; k++ {
						batch := make([]storage.SeriesSamples, nChunks)
						for i := range data {
							id := model.SeriesID(uint64(i+1) << 32)
							start := layout.WindowStart(id, now)
							batch[i] = storage.SeriesSamples{ID: id, Samples: []model.Sample{{T: start + int64(k), V: data[i][k].V}}}
						}
						if _, err := st.Append(ctx, batch); err != nil {
							t.Fatal(err)
						}
					}
				}
				compact := func() { swapAll(t, st, layout) }
				if prior > 0 {
					appendRounds(0, prior)
					compact()
				}
				appendRounds(prior, prior+n)
				before := usedMemory(t, c)
				compact()
				fmt.Printf("%7.0f", float64(usedMemory(t, c)-before)/nChunks)
			}
			fmt.Println()
		}
	}
}

// mainThreadCPU returns the CPU time of the Redis main thread, which runs
// every command and script, in seconds.
func mainThreadCPU(t testing.TB, c redis.UniversalClient) float64 {
	info, err := c.Info(context.Background(), "cpu").Result()
	if err != nil {
		t.Fatal(err)
	}
	var sec float64
	for _, l := range strings.Split(info, "\r\n") {
		for _, f := range []string{"used_cpu_user_main_thread:", "used_cpu_sys_main_thread:"} {
			if v, ok := strings.CutPrefix(l, f); ok {
				x, _ := strconv.ParseFloat(v, 64)
				sec += x
			}
		}
	}
	return sec
}

// topLevelMicros returns the Redis time of the given commands since the
// last CONFIG RESETSTAT. Commands run by scripts are counted under their own
// names and again in the time of the script, so only commands that the
// client sends are passed.
func topLevelMicros(t testing.TB, c redis.UniversalClient, names ...string) float64 {
	info, err := c.Info(context.Background(), "commandstats").Result()
	if err != nil {
		t.Fatal(err)
	}
	var usec float64
	for _, l := range strings.Split(info, "\r\n") {
		name, stats, ok := strings.Cut(l, ":")
		if !ok || !slices.Contains(names, strings.TrimPrefix(name, "cmdstat_")) {
			continue
		}
		for _, kv := range strings.Split(stats, ",") {
			if v, ok := strings.CutPrefix(kv, "usec="); ok {
				x, _ := strconv.ParseFloat(v, 64)
				usec += x
			}
		}
	}
	return usec
}

// TestEvalCapacity compares raw records, compaction and encoding at append
// time with BITFIELD under the same conditions:
//
//   - 12,000 series in 256 shards, written as remote write does: requests of
//     2,000 series with one sample each, so that a script call carries
//     about 8 chunks;
//   - Redis time as the CPU time of the Redis main thread, and as the time
//     of the commands the client sends (scripts included, the commands they
//     run not counted twice);
//   - CPU per sample over the life of a window: every series receives a
//     whole window of 240 samples (1 hour at 15 s), and each shard is
//     compacted every `every` samples at a phase of its own;
//   - memory in steady state with the default 10 minute grace period: the
//     fill of the current window is uniform in 1..240 samples, and one series
//     in six still holds its previous, full window (compacted once more
//     during the grace period).
func TestEvalCapacity(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	const nSeries, perWindow, perRequest = 12000, 240, 2000
	layout := model.Layout{Shards: 256, ChunkRange: time.Hour.Milliseconds(), Grace: 72 * time.Hour.Milliseconds()}
	r := rand.New(rand.NewPCG(21, 22))
	ids := make([]model.SeriesID, nSeries)
	values := make([][]float64, nSeries)
	for i := range ids {
		ids[i] = model.SeriesID(r.Uint64())
		for _, s := range series(gens[i%len(gens)], 2*perWindow, false, uint64(i+31)) {
			values[i] = append(values[i], s.V)
		}
	}
	now := time.Now().UnixMilli()
	type design struct {
		name    string
		every   int       // compaction every this many samples; 0: none
		variant bfVariant // with bitfield
		bf      bool
	}
	designs := []design{
		{name: "raw records"},
		{name: "compaction every 40 samples", every: 40},
		{name: "compaction every 20 samples", every: 20},
		{name: "BITFIELD", variant: bfStateBytes, bf: true},
		{name: "BITFIELD, lean", variant: bfLean, bf: true},
	}
	fmt.Printf("\ncapacity: %d series, requests of %d series, 256 shards\n", nSeries, perRequest)
	fmt.Printf("%-28s %10s %12s %12s %12s %12s\n", "", "B/sample", "append CPU", "compact CPU", "append cmd", "compact cmd")
	for _, d := range designs {
		var (
			st  *Store
			bs  *bfStore
			app func([]storage.SeriesSamples) error
		)
		open := func() {
			flushAll(t, c)
			var err error
			if st, err = New(ctx, c, Options{Prefix: "cap", Layout: layout, Lease: time.Minute}); err != nil {
				t.Fatal(err)
			}
			bs = &bfStore{c: c, prefix: "cap", layout: layout, script: redis.NewScript(bfSource(d.variant))}
			if d.bf {
				app = func(b []storage.SeriesSamples) error { return bs.append(ctx, b) }
			} else {
				app = func(b []storage.SeriesSamples) error { _, err := st.Append(ctx, b); return err }
			}
		}
		// send writes the samples of a round in requests of perRequest series.
		send := func(batch []storage.SeriesSamples) {
			for lo := 0; lo < len(batch); lo += perRequest {
				if err := app(batch[lo:min(lo+perRequest, len(batch))]); err != nil {
					t.Fatal(err)
				}
			}
		}
		compactPhase := func(k int, all bool) {
			for shard := range layout.Shards {
				if !all && (d.every == 0 || (k+1+shard)%d.every != 0) {
					continue
				}
				c.Del(ctx, st.compactLeaseKey(shard))
				if _, err := st.Compact(ctx, shard, time.Hour); err != nil {
					t.Fatal(err)
				}
			}
		}
		sampleAt := func(i int, window int64, k int) model.Sample {
			start := layout.WindowStart(ids[i], now) + window*layout.ChunkRange
			return model.Sample{T: start + 1000 + int64(k)*15_000, V: values[i][k]}
		}

		// CPU over the life of a window.
		open()
		var appendCPU, compactCPU, appendCmd, compactCmd float64
		for k := range perWindow {
			batch := make([]storage.SeriesSamples, nSeries)
			for i := range batch {
				batch[i] = storage.SeriesSamples{ID: ids[i], Samples: []model.Sample{sampleAt(i, 0, k)}}
			}
			_ = c.ConfigResetStat(ctx).Err()
			cpu := mainThreadCPU(t, c)
			send(batch)
			appendCPU += mainThreadCPU(t, c) - cpu
			appendCmd += topLevelMicros(t, c, "eval", "evalsha")
			if d.every > 0 {
				_ = c.ConfigResetStat(ctx).Err()
				cpu := mainThreadCPU(t, c)
				compactPhase(k, false)
				compactCPU += mainThreadCPU(t, c) - cpu
				compactCmd += topLevelMicros(t, c, "eval", "evalsha", "zrangebyscore", "strlen", "mget")
			}
		}
		n := float64(nSeries * perWindow)

		// Memory in steady state.
		open()
		held := 0.0
		for k := range perWindow { // previous windows of one series in six
			var batch []storage.SeriesSamples
			for i := 0; i < nSeries; i += 6 {
				batch = append(batch, storage.SeriesSamples{ID: ids[i], Samples: []model.Sample{sampleAt(i, -1, k)}})
			}
			held += float64(len(batch))
			send(batch)
			compactPhase(k, false)
		}
		if d.every > 0 {
			compactPhase(0, true) // during the grace period
		}
		for k := range perWindow { // current windows, every series ending now
			var batch []storage.SeriesSamples
			for i := range nSeries {
				fill := 1 + i%perWindow
				if j := k - (perWindow - fill); j >= 0 {
					s := sampleAt(i, 0, j)
					s.V = values[i][perWindow+j]
					batch = append(batch, storage.SeriesSamples{ID: ids[i], Samples: []model.Sample{s}})
				}
			}
			held += float64(len(batch))
			send(batch)
			compactPhase(k, false)
		}
		used := usedMemory(t, c)
		// The store started from an empty server: subtract what an empty
		// server holds.
		flushAll(t, c)
		used -= usedMemory(t, c)
		fmt.Printf("%-28s %10.2f %9.2f us %9.2f us %9.2f us %9.2f us   (%.1f samples per series)\n", d.name,
			float64(used)/held, appendCPU*1e6/n, compactCPU*1e6/n, appendCmd/n, compactCmd/n, held/nSeries)
	}
}
