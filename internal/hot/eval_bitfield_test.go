//go:build compressioneval

package hot

// Prototype of Gorilla compression at append time, inside Redis, with
// BITFIELD (docs/hot-compression.md, "Encoding at append time with
// BITFIELD"). Run like eval_compression_test.go.
//
// Chunk key c:<series>:<window>:
//
//	bytes  0..15  fragment (16 ASCII digits, as in the raw format)
//	bytes 16..19  records   u32  every record appended (fence and total)
//	bytes 20..23  nbits     u32  bits of the XOR bit stream
//	bytes 24..31  maxT      i64  latest timestamp of any record (watermark)
//	bytes 32..39  lastT     i64  encoder state
//	bytes 40..47  lastDelta i64
//	bytes 48..55  value     u32 u32
//	bytes 56..57  leading, trailing u8
//	bytes 58..    a Prometheus XOR chunk: u16 count, then the bit stream
//
// Raw key o:<series>:<window>: raw records of samples that cannot extend the
// stream: timestamp <= maxT (out of order, duplicates, overwrites),
// histograms, or a full stream (65535 samples). Reading
// Normalize(stream ++ raw) keeps "the latest write wins": a timestamp enters
// the stream only if no record had it before, and every later record with
// that timestamp is raw.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

const bfStateEnd = 58 // start of the XOR chunk

// The script is assembled from pieces so that variants can be measured.
const bfHead = prelude + `
local frag = tonumber(t[1]) * 1000000 + tonumber(t[2])
local last = tonumber(redis.call('GET', KEYS[3]) or '0') or 0
if frag <= last then frag = last + 1 end
local hdr = string.format('%016.0f', frag)
local maxBytes = tonumber(ARGV[1])
local closed, full = {}, {}
local created = false

local P32 = 4294967296
local function fmt(x) return string.format('%.0f', x) end
local function clz32(x)
  local n = 0
  if x < 65536 then n = n + 16 x = x * 65536 end
  if x < 16777216 then n = n + 8 x = x * 256 end
  if x < 268435456 then n = n + 4 x = x * 16 end
  if x < 1073741824 then n = n + 2 x = x * 4 end
  if x < 2147483648 then n = n + 1 end
  return n
end
local function ctz32(x)
  local n = 0
  if x % 65536 == 0 then n = n + 16 x = x / 65536 end
  if x % 256 == 0 then n = n + 8 x = x / 256 end
  if x % 16 == 0 then n = n + 4 x = x / 16 end
  if x % 4 == 0 then n = n + 2 x = x / 4 end
  if x % 2 == 0 then n = n + 1 end
  return n
end

-- Bit writer: accumulates up to 52 bits (exact in a double) per BITFIELD SET.
local key, ops, pos, acc, accBits
local function flush()
  if accBits > 0 then
    ops[#ops + 1] = 'SET'
    ops[#ops + 1] = 'u' .. accBits
    ops[#ops + 1] = pos
    ops[#ops + 1] = fmt(acc)
    pos = pos + accBits
    acc, accBits = 0, 0
    if #ops >= 4000 then
      redis.call('BITFIELD', key, unpack(ops))
      ops = {}
    end
  end
end
local function put(v, w)
  if accBits + w > 52 then flush() end
  acc = acc * 2 ^ w + v
  accBits = accBits + w
end
-- Writes the 64-bit h:l shifted right by tr, as 64 - ld - tr bits.
local function putShifted(h, l, ld, tr)
  local sig = 64 - ld - tr
  if tr >= 32 then
    put(math.floor(h / 2 ^ (tr - 32)), sig)
  else
    local p = 2 ^ tr
    local low = (h % p) * 2 ^ (32 - tr) + math.floor(l / p)
    if sig > 32 then
      put(math.floor(h / p), sig - 32)
      put(low, 32)
    else
      put(low, sig)
    end
  end
end

for c = 0, (#KEYS - 3) / 3 - 1 do
  key = KEYS[4 + 3 * c]
  local okey, windows = KEYS[5 + 3 * c], KEYS[6 + 3 * c]
  local j = 2 + 4 * c
  local member, start, due, data = ARGV[j], tonumber(ARGV[j + 1]), tonumber(ARGV[j + 2]), ARGV[j + 3]
  if now >= due or redis.call('ZSCORE', KEYS[2], member) then
    closed[#closed + 1] = c
  else
`

// Existence and size check: STRLEN of both keys, as the raw format does.
const bfCheckStrlen = `
    local clen = redis.call('STRLEN', key)
    local exists = clen > 0
    if (clen == 0 and 60 or clen) + redis.call('STRLEN', okey) + string.len(data) > maxBytes then
      full[#full + 1] = c
    else
`

// Lean check: the state read doubles as the existence check, and the size
// limit is not checked (lower bound of the cost).
const bfCheckLean = `
    local st = redis.call('GETRANGE', key, 16, 59)
    local exists = st ~= ''
    do
`

const bfCreate = `
      local records, nbits, maxT, lastT, lastD, phi, plo, lead, trail, count
      if not exists then
        redis.call('SET', key, hdr)
        redis.call('ZADD', KEYS[1], due, member)
        redis.call('ZADD', windows, start, start)
        created = true
        records, nbits, maxT, lastT, lastD, phi, plo, lead, trail, count = 0, 0, start - 1, 0, 0, 0, 0, 255, 0, 0
      else
`

const bfReadBitfield = `
        local s = redis.call('BITFIELD', key, 'GET', 'u32', 128, 'GET', 'u32', 160, 'GET', 'i64', 192,
          'GET', 'i64', 256, 'GET', 'i64', 320, 'GET', 'u32', 384, 'GET', 'u32', 416,
          'GET', 'u8', 448, 'GET', 'u8', 456, 'GET', 'u16', 464)
        records, nbits, maxT, lastT, lastD, phi, plo, lead, trail, count =
          s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], s[10]
      end
`

const bfReadGetrange = `
        records, nbits, maxT, lastT, lastD, phi, plo, lead, trail, count =
          struct.unpack('>I4I4i8i8i8I4I4BBH', redis.call('GETRANGE', key, 16, 59))
      end
`

const bfReadLean = `
        records, nbits, maxT, lastT, lastD, phi, plo, lead, trail, count = struct.unpack('>I4I4i8i8i8I4I4BBH', st)
      end
`

const bfRecordsHead = `
      ops, pos, acc, accBits = {}, 480 + nbits, 0, 0
      local raw = {}
      local off = 1
      while off <= string.len(data) do
        local thi, tlo, vhi, vlo = struct.unpack('>I4I4I4I4', data, off)
        local ts = thi * P32 + tlo
        local rlen = 16
        local hist = math.floor(vhi / 65536) == 32756 -- 0x7ff4
        if hist then
          rlen = 16 + math.ceil(((vhi % 65536) * P32 + vlo) / 16) * 16
        end
        records = records + 1
`

// bfEncode appends a sample to the stream, or its record to the raw key.
const bfEncode = `
        if hist or ts <= maxT or count >= 65535 then
          raw[#raw + 1] = string.sub(data, off, off + rlen - 1)
        else
          if count == 0 then
            local zz = 2 * ts -- zigzag varint; timestamps are positive
            while zz >= 128 do put(zz % 128 + 128, 8) zz = math.floor(zz / 128) end
            put(zz, 8)
            put(vhi, 32)
            put(vlo, 32)
            lastD = 0
          else
            local d = ts - lastT
            if count == 1 then
              local u = d
              while u >= 128 do put(u % 128 + 128, 8) u = math.floor(u / 128) end
              put(u, 8)
            else
              local dod = d - lastD
              if dod == 0 then put(0, 1)
              elseif dod >= -8191 and dod <= 8192 then put(2, 2) put(dod % 16384, 14)
              elseif dod >= -65535 and dod <= 65536 then put(6, 3) put(dod % 131072, 17)
              elseif dod >= -524287 and dod <= 524288 then put(14, 4) put(dod % 1048576, 20)
              else put(15, 4) put(math.floor(dod / P32) % P32, 32) put(dod % P32, 32) end
            end
            lastD = d
            local xh, xl = bit.bxor(vhi, phi), bit.bxor(vlo, plo)
            if xh < 0 then xh = xh + P32 end
            if xl < 0 then xl = xl + P32 end
            if xh == 0 and xl == 0 then
              put(0, 1)
            else
              put(1, 1)
              local nl, nt
              if xh ~= 0 then nl = clz32(xh) else nl = 32 + clz32(xl) end
              if xl ~= 0 then nt = ctz32(xl) else nt = 32 + ctz32(xh) end
              if nl >= 32 then nl = 31 end
              if lead ~= 255 and nl >= lead and nt >= trail then
                put(0, 1)
                putShifted(xh, xl, lead, trail)
              else
                lead, trail = nl, nt
                put(1, 1)
                put(nl, 5)
                put((64 - nl - nt) % 64, 6)
                putShifted(xh, xl, nl, nt)
              end
            end
          end
          lastT, phi, plo, count = ts, vhi, vlo, count + 1
        end
`

const bfRecordsTail = `
        if ts > maxT then maxT = ts end
        off = off + rlen
      end
      flush()
`

const bfWriteBitfield = `
      local o = ops
      o[#o + 1] = 'SET' o[#o + 1] = 'u32' o[#o + 1] = 128 o[#o + 1] = fmt(records)
      o[#o + 1] = 'SET' o[#o + 1] = 'u32' o[#o + 1] = 160 o[#o + 1] = fmt(pos - 480)
      o[#o + 1] = 'SET' o[#o + 1] = 'i64' o[#o + 1] = 192 o[#o + 1] = fmt(maxT)
      o[#o + 1] = 'SET' o[#o + 1] = 'i64' o[#o + 1] = 256 o[#o + 1] = fmt(lastT)
      o[#o + 1] = 'SET' o[#o + 1] = 'i64' o[#o + 1] = 320 o[#o + 1] = fmt(lastD)
      o[#o + 1] = 'SET' o[#o + 1] = 'u32' o[#o + 1] = 384 o[#o + 1] = fmt(phi)
      o[#o + 1] = 'SET' o[#o + 1] = 'u32' o[#o + 1] = 416 o[#o + 1] = fmt(plo)
      o[#o + 1] = 'SET' o[#o + 1] = 'u8' o[#o + 1] = 448 o[#o + 1] = lead
      o[#o + 1] = 'SET' o[#o + 1] = 'u8' o[#o + 1] = 456 o[#o + 1] = trail
      o[#o + 1] = 'SET' o[#o + 1] = 'u16' o[#o + 1] = 464 o[#o + 1] = count
      redis.call('BITFIELD', key, unpack(o))
`

const bfWriteBits = `
      if #ops > 0 then redis.call('BITFIELD', key, unpack(ops)) end
`

const bfWriteSetrange = `
      redis.call('SETRANGE', key, 16, struct.pack('>I4I4i8i8i8I4I4BBH', records, pos - 480, maxT, lastT, lastD, phi, plo, lead, trail, count))
`

const bfTail = `
      if #raw > 0 then redis.call('APPEND', okey, table.concat(raw)) end
    end
  end
end
if created then redis.call('SET', KEYS[3], hdr) end
return {closed, full}
`

// bfVariant selects how the prototype handles its state.
type bfVariant int

const (
	// bfAllBitfield reads and writes the state fields with BITFIELD.
	bfAllBitfield bfVariant = iota
	// bfStateBytes reads and writes the state as bytes (GETRANGE,
	// SETRANGE and struct) and the bit stream with BITFIELD.
	bfStateBytes
	// bfStateBytesNoBits is bfStateBytes without writing the bits.
	bfStateBytesNoBits
	// bfStateOnly is bfStateBytes without encoding: the state handling
	// alone (every record goes to the raw key).
	bfStateOnly
	// bfLean is bfStateBytes without the STRLEN calls.
	bfLean
)

var bfVariantNames = map[bfVariant]string{
	bfAllBitfield:      "state and bits via BITFIELD",
	bfStateBytes:       "state via GETRANGE/SETRANGE, bits via BITFIELD",
	bfStateBytesNoBits: "  ... without writing the bits",
	bfStateOnly:        "  ... without encoding (state only)",
	bfLean:             "lean: no STRLEN, state read doubles as EXISTS",
}

func bfSource(v bfVariant) string {
	check, read, encode, write := bfCheckStrlen, bfReadGetrange, bfEncode, bfWriteBits+bfWriteSetrange
	switch v {
	case bfAllBitfield:
		read, write = bfReadBitfield, bfWriteBitfield
	case bfStateBytesNoBits:
		write = bfWriteSetrange
	case bfStateOnly:
		encode, write = `
        raw[#raw + 1] = string.sub(data, off, off + rlen - 1)
`, bfWriteSetrange
	case bfLean:
		check, read = bfCheckLean, bfReadLean
	}
	return bfHead + check + bfCreate + read + bfRecordsHead + encode + bfRecordsTail + write + bfTail
}

var appendBF = redis.NewScript(bfSource(bfAllBitfield))

type bfStore struct {
	c      redis.UniversalClient
	prefix string
	layout model.Layout
	script *redis.Script // appendBF if nil
}

func (s *bfStore) keys(id model.SeriesID, start int64) (c, o, w string) {
	tag := fmt.Sprintf("%s:{%d}", s.prefix, s.layout.Shard(id))
	m := member(id, start)
	return tag + ":c:" + m, tag + ":o:" + m, tag + ":w:" + id.String()
}

// append mirrors Store.Append: one script call per shard and up to 256
// chunks per call.
func (s *bfStore) append(ctx context.Context, batch []storage.SeriesSamples) error {
	type pending struct {
		id         model.SeriesID
		start, due int64
		data       []byte
	}
	shards := map[int][]*pending{}
	byMember := map[string]*pending{}
	for _, ss := range batch {
		for _, smp := range ss.Samples {
			start := s.layout.WindowStart(ss.ID, smp.T)
			m := member(ss.ID, start)
			p := byMember[m]
			if p == nil {
				p = &pending{id: ss.ID, start: start, due: s.layout.Due(start)}
				byMember[m] = p
				shards[s.layout.Shard(ss.ID)] = append(shards[s.layout.Shard(ss.ID)], p)
			}
			p.data = chunk.AppendRaw(p.data, []model.Sample{smp})
		}
	}
	var calls []scriptCall
	for shard, ps := range shards {
		tag := fmt.Sprintf("%s:{%d}", s.prefix, shard)
		for len(ps) > 0 {
			n := min(len(ps), maxChunksPerCall)
			call := scriptCall{keys: []string{tag + ":due", tag + ":inf", tag + ":clock"}, args: []any{DefaultMaxChunkBytes}}
			for _, p := range ps[:n] {
				ck, ok, wk := s.keys(p.id, p.start)
				call.keys = append(call.keys, ck, ok, wk)
				call.args = append(call.args, member(p.id, p.start), p.start, p.due, p.data)
			}
			calls = append(calls, call)
			ps = ps[n:]
		}
	}
	script := s.script
	if script == nil {
		script = appendBF
	}
	res, err := (&Store{client: s.c}).runScripts(ctx, script, calls)
	if err != nil {
		return err
	}
	for _, r := range res {
		reply, _ := r.Slice()
		if len(reply) != 2 || len(reply[0].([]any)) > 0 || len(reply[1].([]any)) > 0 {
			return fmt.Errorf("rejected: %v", reply)
		}
	}
	return nil
}

// decodeBF decodes a chunk and its raw key.
func decodeBF(c, o []byte) (samples []model.Sample, records int, err error) {
	if len(c) < bfStateEnd+2 {
		return nil, 0, fmt.Errorf("short chunk of %d bytes", len(c))
	}
	records = int(binary.BigEndian.Uint32(c[16:20]))
	nbits := int(binary.BigEndian.Uint32(c[20:24]))
	if want := bfStateEnd + 2 + (nbits+7)/8; len(c) != want {
		return nil, 0, fmt.Errorf("chunk is %d bytes, want %d", len(c), want)
	}
	xc, err := chunkenc.FromData(chunkenc.EncXOR, c[bfStateEnd:])
	if err != nil {
		return nil, 0, err
	}
	it := xc.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		t, v := it.At()
		samples = append(samples, model.Sample{T: t, V: v})
	}
	if err := it.Err(); err != nil {
		return nil, 0, err
	}
	raw, err := chunk.DecodeRaw(o)
	if err != nil {
		return nil, 0, err
	}
	return chunk.Normalize(append(samples, raw...)), records, nil
}

// read returns the samples and records of a window, and the chunk value.
func (s *bfStore) read(ctx context.Context, id model.SeriesID, start int64) ([]model.Sample, int, []byte, error) {
	ck, ok, _ := s.keys(id, start)
	v, err := s.c.MGet(ctx, ck, ok).Result()
	if err != nil {
		return nil, 0, nil, err
	}
	bytesOf := func(x any) []byte {
		if x == nil {
			return nil
		}
		return []byte(x.(string))
	}
	samples, records, err := decodeBF(bytesOf(v[0]), bytesOf(v[1]))
	return samples, records, bytesOf(v[0]), err
}

func sameSamples(a, b []model.Sample) error {
	if len(a) != len(b) {
		return fmt.Errorf("%d samples, want %d", len(a), len(b))
	}
	for i := range a {
		switch {
		case a[i].T != b[i].T:
			return fmt.Errorf("%d: t %d, want %d", i, a[i].T, b[i].T)
		case (a[i].H == nil) != (b[i].H == nil):
			return fmt.Errorf("%d: histogram mismatch", i)
		case a[i].H != nil:
			if !a[i].H.Equals(b[i].H) {
				return fmt.Errorf("%d: histogram differs", i)
			}
		case math.Float64bits(a[i].V) != math.Float64bits(b[i].V):
			return fmt.Errorf("%d: v %x, want %x", i, math.Float64bits(a[i].V), math.Float64bits(b[i].V))
		}
	}
	return nil
}

// bfLayout keeps windows open for days, so that tests can write anywhere in
// the previous window.
func bfLayout(shards int) model.Layout {
	return model.Layout{Shards: shards, ChunkRange: 24 * time.Hour.Milliseconds(), Grace: 72 * time.Hour.Milliseconds()}
}

// TestEvalBitfieldMatchesPrometheus appends in-order series, one sample per
// call, and checks that the stream is byte for byte the Prometheus XOR
// chunk of the same samples.
func TestEvalBitfieldMatchesPrometheus(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	flushAll(t, c)
	s := &bfStore{c: c, prefix: "bf", layout: bfLayout(4)}
	r := rand.New(rand.NewPCG(5, 6))
	now := time.Now().UnixMilli()
	kinds := append(slices.Clone(gens),
		gen{"random bits", func(r *rand.Rand, _ float64) float64 {
			return math.Float64frombits(r.Uint64()&^(0xffff<<48) | 0x3ff0<<48)
		}, 1},
		gen{"special values", func(r *rand.Rand, _ float64) float64 {
			return []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1),
				math.Float64frombits(0x7ff0000000000002), 1e-300, math.MaxFloat64, -1}[r.IntN(9)]
		}, 0})
	checked := 0
	for _, g := range kinds {
		for rep := range 20 {
			id := model.SeriesID(r.Uint64())
			start := s.layout.WindowStart(id, now) - s.layout.ChunkRange
			ts := start + int64(r.IntN(1000))
			v := g.init
			want := chunkenc.NewXORChunk()
			app, _ := want.Appender()
			for range 1 + r.IntN(300) {
				smp := model.Sample{T: ts, V: chunk.CanonicalValue(v)}
				if err := s.append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{smp}}}); err != nil {
					t.Fatal(err)
				}
				app.Append(0, smp.T, smp.V)
				v = g.next(r, v)
				// Regular, jittered, or with gaps that need 17, 20 and 64
				// bit delta-of-deltas.
				switch rep % 4 {
				case 0:
					ts += 15000
				case 1:
					ts += 15000 + int64(r.IntN(9)-4)
				case 2:
					ts += []int64{1, 15000, 70_000, 500_000, 3_000_000}[r.IntN(5)]
				case 3:
					ts += 1 + int64(r.IntN(100_000))
				}
				if s.layout.WindowStart(id, ts) != start {
					break
				}
			}
			_, _, value, err := s.read(ctx, id, start)
			if err != nil {
				t.Fatalf("%s: %v", g.name, err)
			}
			if got := value[bfStateEnd:]; string(got) != string(want.Bytes()) {
				t.Fatalf("%s rep %d: stream differs from Prometheus (%d vs %d bytes)", g.name, rep, len(got), len(want.Bytes()))
			}
			checked++
		}
	}
	fmt.Printf("\nbyte-identical to Prometheus XOR chunks: %d series\n", checked)
}

func evalHistogram(i int) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Schema: 1, ZeroThreshold: 0.001, ZeroCount: float64(i), Count: float64(10 + 3*i), Sum: float64(i) * 1.5,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{float64(5 + i), float64(5 + 2*i)},
	}
}

// TestEvalBitfieldSemantics appends samples in random order, with
// duplicates, overwrites, special values and histograms, in random batches,
// and checks that reads return exactly what the raw format returns.
func TestEvalBitfieldSemantics(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	flushAll(t, c)
	s := &bfStore{c: c, prefix: "bs", layout: bfLayout(4)}
	r := rand.New(rand.NewPCG(7, 8))
	now := time.Now().UnixMilli()
	const iterations = 300
	for iter := range iterations {
		id := model.SeriesID(r.Uint64())
		start := s.layout.WindowStart(id, now-2*3_600_000)
		t0 := start + int64(r.IntN(1000))
		var all []model.Sample
		n := 1 + r.IntN(400)
		for i := range n {
			var ts int64
			switch {
			case iter%5 == 0: // fully random order
				ts = t0 + int64(r.IntN(n))*15000
			case r.IntN(10) == 0 && len(all) > 0: // duplicate or overwrite
				ts = all[r.IntN(len(all))].T
			default:
				ts = t0 + int64(i)*15000 + int64(r.IntN(3))
			}
			v := []float64{float64(i), r.Float64(), math.NaN(), math.Float64frombits(0x7ff0000000000002),
				math.Float64frombits(0x7ff4000000000001)}[r.IntN(5)]
			smp := model.Sample{T: ts, V: chunk.CanonicalValue(v)}
			if r.IntN(15) == 0 {
				smp = model.Sample{T: ts, H: evalHistogram(i)}
			}
			all = append(all, smp)
		}
		for i := 0; i < len(all); {
			k := 1 + r.IntN(8)
			if err := s.append(ctx, []storage.SeriesSamples{{ID: id, Samples: all[i:min(i+k, len(all))]}}); err != nil {
				t.Fatal(err)
			}
			i += k
		}
		got, records, _, err := s.read(ctx, id, start)
		if err != nil {
			t.Fatal(err)
		}
		_, want, wantRecords, err := chunk.DecodeHot(append(slices.Clone(evalHeader), chunk.AppendRaw(nil, all)...))
		if err != nil {
			t.Fatal(err)
		}
		if records != wantRecords {
			t.Fatalf("iteration %d: %d records, want %d", iter, records, wantRecords)
		}
		if err := sameSamples(got, want); err != nil {
			t.Fatalf("iteration %d: %v", iter, err)
		}
	}
	fmt.Printf("\nreads identical to the raw format: %d series\n", iterations)
}

// measureAppend reports the Redis time per sample of the append function
// that setup returns, with one sample per chunk and 256 chunks per call.
func measureAppend(t *testing.T, c redis.UniversalClient, g gen, name string, setup func() func([]storage.SeriesSamples) error) {
	ctx := context.Background()
	const nChunks, rounds = 256, 300
	flushAll(t, c)
	app := setup()
	r := rand.New(rand.NewPCG(9, 10))
	ids := make([]model.SeriesID, nChunks)
	vals := make([]float64, nChunks)
	for i := range ids {
		ids[i] = model.SeriesID(r.Uint64())
		vals[i] = g.init
	}
	now := time.Now().UnixMilli()
	_ = c.ConfigResetStat(ctx).Err()
	for k := range rounds {
		batch := make([]storage.SeriesSamples, nChunks)
		for i, id := range ids {
			batch[i] = storage.SeriesSamples{ID: id, Samples: []model.Sample{{T: now - int64(rounds-k)*15000, V: vals[i]}}}
			vals[i] = g.next(r, vals[i])
		}
		if err := app(batch); err != nil {
			t.Fatal(err)
		}
	}
	usec, calls := scriptMicros(t, c)
	fmt.Printf("  %-48s %5.2f us/sample\n", name, usec/(calls*nChunks))
}

// TestEvalBitfieldCost compares the Redis time of the current append script
// and of the variants of the prototype.
func TestEvalBitfieldCost(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	layout := bfLayout(1)
	for _, gi := range []int{0, 2, 6} {
		g := gens[gi]
		fmt.Printf("\n%s\n", g.name)
		measureAppend(t, c, g, "current (APPEND raw)", func() func([]storage.SeriesSamples) error {
			st, err := New(ctx, c, Options{Prefix: "cur", Layout: layout, Lease: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			return func(b []storage.SeriesSamples) error { _, err := st.Append(ctx, b); return err }
		})
		for v := bfAllBitfield; v <= bfLean; v++ {
			bs := &bfStore{c: c, prefix: "bfc", layout: layout, script: redis.NewScript(bfSource(v))}
			measureAppend(t, c, g, bfVariantNames[v], func() func([]storage.SeriesSamples) error {
				return func(b []storage.SeriesSamples) error { return bs.append(ctx, b) }
			})
		}
	}
}

// TestEvalBitfieldMemory measures memory in steady state (chunk fill
// uniform in 1..240 samples), appending one sample per series per call.
func TestEvalBitfieldMemory(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	const nSeries = 12000
	layout := bfLayout(256)
	data := make([][]model.Sample, nSeries)
	ids := make([]model.SeriesID, nSeries)
	now := time.Now().UnixMilli()
	n := 0.0
	r := rand.New(rand.NewPCG(11, 12))
	for i := range data {
		ids[i] = model.SeriesID(r.Uint64())
		g := gens[i%len(gens)]
		v := g.init
		start := layout.WindowStart(ids[i], now-time.Hour.Milliseconds())
		for k := range 1 + i%240 {
			data[i] = append(data[i], model.Sample{T: start + 1000 + int64(k)*15000, V: v})
			v = g.next(r, v)
		}
		n += float64(len(data[i]))
	}
	fmt.Println()
	for _, mode := range []string{"current", "BITFIELD"} {
		flushAll(t, c)
		base := usedMemory(t, c)
		st, err := New(ctx, c, Options{Prefix: "m", Layout: layout, Lease: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		bs := &bfStore{c: c, prefix: "m", layout: layout}
		for k := range 240 {
			var batch []storage.SeriesSamples
			for i := range data {
				if k < len(data[i]) {
					batch = append(batch, storage.SeriesSamples{ID: ids[i], Samples: data[i][k : k+1]})
				}
			}
			if mode == "current" {
				_, err = st.Append(ctx, batch)
			} else {
				err = bs.append(ctx, batch)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		used := usedMemory(t, c) - base
		keys, _ := c.Keys(ctx, "m:{*}:c:*").Result()
		var payload, overhead int64
		for i, k := range keys {
			l, _ := c.StrLen(ctx, k).Result()
			payload += l
			if i < 500 {
				u, _ := c.MemoryUsage(ctx, k, 0).Result()
				overhead += u - l
			}
		}
		fmt.Printf("%-9s whole store %5.2f B/sample, chunk values %5.2f B/sample, MEMORY USAGE - STRLEN %4.0f B/chunk\n",
			mode, float64(used)/n, float64(payload)/n, float64(overhead)/500)
	}
}

// casBF is a cost model of client-side encoding: the ingester computes the
// bits from state it caches; Redis only compares the version (records,
// nbits), writes the bits with BITFIELD and the new state with SETRANGE.
// Only the hit path is modelled.
var casBF = redis.NewScript(prelude + `
local hits = 0
for c = 0, (#KEYS - 2) / 2 - 1 do
  local key = KEYS[3 + 2 * c]
  local j = 1 + 6 * c
  local member, due, want, bits, width, state = ARGV[j], tonumber(ARGV[j + 1]), ARGV[j + 2], ARGV[j + 3], ARGV[j + 4], ARGV[j + 5]
  if now < due and not redis.call('ZSCORE', KEYS[2], member) then
    if redis.call('GETRANGE', key, 16, 23) == want then
      local nbits = struct.unpack('>I4', state, 5)
      redis.call('BITFIELD', key, 'SET', 'u' .. width, 480 + nbits - width, bits)
      redis.call('SETRANGE', key, 16, state)
      hits = hits + 1
    else
      redis.call('APPEND', KEYS[4 + 2 * c], want) -- miss: a raw record
    end
  end
end
return hits
`)

// TestEvalClientSideEncodingCost measures the hit path of casBF.
func TestEvalClientSideEncodingCost(t *testing.T) {
	c := evalClient(t)
	ctx := context.Background()
	flushAll(t, c)
	const nChunks, rounds = 256, 300
	due := time.Now().Add(time.Hour).UnixMilli()
	type version struct{ records, nbits uint32 }
	versions := make([]version, nChunks)
	p := c.Pipeline()
	for i := range nChunks {
		p.Set(ctx, fmt.Sprintf("cas:{0}:c:%d", i), string(evalHeader)+strings.Repeat("\x00", 44), 0)
	}
	if _, err := p.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	_ = c.ConfigResetStat(ctx).Err()
	for range rounds {
		keys := []string{"cas:{0}:due", "cas:{0}:inf"}
		var args []any
		for i := range nChunks {
			keys = append(keys, fmt.Sprintf("cas:{0}:c:%d", i), fmt.Sprintf("cas:{0}:o:%d", i))
			v := &versions[i]
			want := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, v.records), v.nbits)
			width := 2 + i%40 // from 2 bits (same delta, same value) to 41
			v.records++
			v.nbits += uint32(width)
			state := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, v.records), v.nbits)
			state = append(state, make([]byte, 36)...)
			args = append(args, "m"+strconv.Itoa(i), due, want, 1, width, state)
		}
		if n, err := casBF.Run(ctx, c, keys, args...).Int(); err != nil || n != nChunks {
			t.Fatal(n, err)
		}
	}
	usec, calls := scriptMicros(t, c)
	fmt.Printf("\nclient-side encoding, compare + BITFIELD write (hit path): %.2f us/sample\n", usec/(calls*nChunks))
}
