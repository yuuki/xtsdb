// Package hot implements the in-memory tier on Redis (or Valkey), in
// standalone, Sentinel or Cluster mode.
//
// Key layout for shard N (all keys of a shard share the hash tag {N}):
//
//	<prefix>:{N}:c:<series>:<window>  chunk: 16-byte header + raw samples
//	<prefix>:{N}:z:<series>:<window>  compacted part of the chunk
//	<prefix>:{N}:due                  sorted set: chunk member -> due time
//	<prefix>:{N}:inf                  sorted set: chunk member -> lease expiry
//	<prefix>:{N}:infdue               sorted set: in-flight member -> due time
//	<prefix>:{N}:clock                last fragment issued in the shard
//	<prefix>:{N}:w:<series>           sorted set: window starts of the series' chunks
//	<prefix>:{N}:compact              compaction lease of the shard
//	<prefix>:{N}:q:<series>:<window>:<digest>  quarantined (undecodable) chunk
//	<prefix>:{N}:qz:<series>:<window>:<digest> its compacted part
//	<prefix>:{N}:qidx                 sorted set: quarantine key -> expiry time
//	<prefix>:meta                     hash: layout parameters
package hot

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Limits on the size of a single script invocation and pipeline.
const (
	maxChunksPerCall = 256
	maxPipelineCmds  = 1024
)

// SetLogger routes the Redis client library's own log lines to slog.
func SetLogger(l *slog.Logger) {
	redis.SetLogger(redisLogger{l.With("component", "redis")})
}

type redisLogger struct{ l *slog.Logger }

func (r redisLogger) Printf(ctx context.Context, format string, v ...any) {
	r.l.WarnContext(ctx, fmt.Sprintf(format, v...))
}

// NewClient creates a Redis client from configuration.
func NewClient(cfg config.RedisConfig) (redis.UniversalClient, error) {
	tlsCfg, err := cfg.TLS.Build()
	if err != nil {
		return nil, fmt.Errorf("redis tls: %w", err)
	}
	if slices.Contains(cfg.Addrs, "") {
		return nil, errors.New("redis.addrs contains an empty address")
	}
	if (cfg.ClusterMode || len(cfg.Addrs) > 1) && cfg.MasterName == "" && cfg.DB != 0 {
		return nil, errors.New("redis.db must be 0 in cluster mode")
	}
	return redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:         cfg.Addrs,
		IsClusterMode: cfg.ClusterMode,
		MasterName:    cfg.MasterName,
		ClientName:    "xtsdb",
		Username:      cfg.Username,
		Password:      cfg.Password,
		DB:            cfg.DB,
		PoolSize:      cfg.PoolSize,
		DialTimeout:   cfg.DialTimeout.D(),
		ReadTimeout:   cfg.ReadTimeout.D(),
		WriteTimeout:  cfg.WriteTimeout.D(),
		TLSConfig:     tlsCfg,
	}), nil
}

// DefaultMaxChunkBytes bounds the size of one chunk (about 2 million
// samples). It keeps every chunk well below the Redis string limit and
// fast to load; samples beyond it are rejected.
const DefaultMaxChunkBytes = 32 << 20

// Options configures a Store.
type Options struct {
	Prefix        string
	Layout        model.Layout
	Lease         time.Duration
	MaxChunkBytes int // DefaultMaxChunkBytes if zero
	// QuarantineTTL is how long undecodable chunks are kept for inspection;
	// zero keeps them until they are deleted.
	QuarantineTTL time.Duration
	// Backend is the persistent backend (storage.backend) that chunks are
	// flushed to. It is recorded like the layout, so that a deployment does
	// not switch backends and split its series between two stores. Empty
	// skips the check.
	Backend string
	Logger  *slog.Logger
}

// Store is a storage.HotStore backed by Redis.
type Store struct {
	client        redis.UniversalClient
	prefix        string
	layout        model.Layout
	lease         time.Duration
	maxChunkBytes int
	quarantineTTL time.Duration
	backend       string
	logger        *slog.Logger

	// compactMargin is storage.CompactMargin, shorter in tests.
	compactMargin time.Duration
	// compactMinRaw is the size of the raw records a chunk needs to be
	// compacted: storage.CompactMinRecords records, fewer in tests.
	compactMinRaw int
	// compactPage is how many open chunks are listed at a time.
	compactPage int
	// beforeCompact runs between the reads and the writes of a compaction;
	// tests use it to change chunks in between.
	beforeCompact func()
}

var _ storage.HotStore = (*Store)(nil)

// New creates a Store and checks that the layout matches the one already
// recorded in Redis.
func New(ctx context.Context, client redis.UniversalClient, opts Options) (*Store, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.MaxChunkBytes <= 0 {
		opts.MaxChunkBytes = DefaultMaxChunkBytes
	}
	s := &Store{
		client:        client,
		prefix:        opts.Prefix,
		layout:        opts.Layout,
		lease:         opts.Lease,
		maxChunkBytes: opts.MaxChunkBytes,
		quarantineTTL: opts.QuarantineTTL,
		backend:       opts.Backend,
		logger:        opts.Logger,
		compactMargin: storage.CompactMargin,
		compactMinRaw: storage.CompactMinRecords * chunk.RecordLen,
		compactPage:   1000,
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	if err := s.checkLayout(ctx); err != nil {
		return nil, err
	}
	if err := s.buildWindowIndex(ctx); err != nil {
		return nil, err
	}
	s.warnOnEviction(ctx)
	return s, nil
}

// checkLayout records the layout on first use and refuses to run with a
// different one: changing the shard count or the chunk range would make
// existing chunks unreachable, and changing the persistent backend would
// leave the series flushed so far, and their index, in the other one.
//
// Once the layout is recorded this only reads, so that a flusher can start
// while Redis is out of memory: flushing is what frees it.
func (s *Store) checkLayout(ctx context.Context) error {
	key := s.prefix + ":meta"
	fields := []struct{ name, value, option string }{
		{"shards", strconv.Itoa(s.layout.Shards), "storage.shards"},
		{"chunk_range_ms", strconv.FormatInt(s.layout.ChunkRange, 10), "storage.chunk_range"},
	}
	if s.backend != "" {
		fields = append(fields, struct{ name, value, option string }{"backend", s.backend, "storage.backend"})
	}
	for _, f := range fields {
		got, err := s.client.HGet(ctx, key, f.name).Result()
		if errors.Is(err, redis.Nil) {
			if err := s.client.HSetNX(ctx, key, f.name, f.value).Err(); err != nil {
				return fmt.Errorf("redis meta: %w", err)
			}
			got, err = s.client.HGet(ctx, key, f.name).Result()
		}
		if err != nil {
			return fmt.Errorf("redis meta: %w", err)
		}
		if got != f.value {
			return fmt.Errorf("%s is %s but the data in Redis was written with %s (%s); "+
				"changing it is not supported", f.option, f.value, got, f.name)
		}
	}
	return nil
}

// windowIndexField marks in <prefix>:meta that every chunk is registered in
// the window index of its series.
const windowIndexField = "window_index"

// buildWindowIndex registers the chunks of earlier versions, which did not
// maintain window indexes, once. Every chunk is a member of the due set or
// of the in-flight set of its shard.
func (s *Store) buildWindowIndex(ctx context.Context) error {
	key := s.prefix + ":meta"
	done, err := s.client.HExists(ctx, key, windowIndexField).Result()
	if err != nil {
		return fmt.Errorf("redis meta: %w", err)
	}
	if done {
		return nil
	}
	n := 0
	for shard := range s.layout.Shards {
		var members []string
		for _, set := range []string{s.dueKey(shard), s.inflightKey(shard)} {
			m, err := s.client.ZRange(ctx, set, 0, -1).Result()
			if err != nil {
				return fmt.Errorf("build window index: %w", err)
			}
			members = append(members, m...)
		}
		var calls []scriptCall
		for len(members) > 0 {
			batch := members[:min(len(members), maxChunksPerCall)]
			members = members[len(batch):]
			var call scriptCall
			for _, m := range batch {
				if id, start, err := parseMember(m); err == nil {
					call.keys = append(call.keys, s.chunkKey(shard, m), s.windowsKey(shard, id))
					call.args = append(call.args, start)
				}
			}
			if len(call.keys) > 0 {
				calls = append(calls, call)
			}
		}
		results, err := s.runScripts(ctx, indexScript, calls)
		if err != nil {
			return fmt.Errorf("build window index: %w", err)
		}
		for _, r := range results {
			k, _ := r.Int()
			n += k
		}
	}
	if err := s.client.HSet(ctx, key, windowIndexField, "1").Err(); err != nil {
		return fmt.Errorf("redis meta: %w", err)
	}
	if n > 0 {
		s.logger.Info("registered existing chunks in the window index", "chunks", n)
	}
	return nil
}

// warnOnEviction logs a warning if Redis may evict keys, which would lose
// samples that have not been flushed yet.
func (s *Store) warnOnEviction(ctx context.Context) {
	res, err := s.client.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return // CONFIG is often disabled on managed services
	}
	if p, ok := res["maxmemory-policy"]; ok && p != "noeviction" {
		s.logger.Warn("Redis maxmemory-policy is not noeviction; unflushed samples may be evicted", "policy", p)
	}
}

func (s *Store) tag(shard int) string { return s.prefix + ":{" + strconv.Itoa(shard) + "}" }

func (s *Store) dueKey(shard int) string         { return s.tag(shard) + ":due" }
func (s *Store) inflightKey(shard int) string    { return s.tag(shard) + ":inf" }
func (s *Store) inflightDueKey(shard int) string { return s.tag(shard) + ":infdue" }
func (s *Store) clockKey(shard int) string       { return s.tag(shard) + ":clock" }
func (s *Store) chunkKey(shard int, member string) string {
	return s.tag(shard) + ":c:" + member
}

// compactedKey holds the compacted part of a chunk.
func (s *Store) compactedKey(shard int, member string) string {
	return s.tag(shard) + ":z:" + member
}

func (s *Store) compactLeaseKey(shard int) string { return s.tag(shard) + ":compact" }

// windowsKey is the window index of a series: the starts of the windows it
// has chunks for, so that reads touch only chunks that exist.
func (s *Store) windowsKey(shard int, id model.SeriesID) string {
	return s.tag(shard) + ":w:" + id.String()
}

// quarantineKey names a quarantined chunk after its content (the length of
// the chunk, the chunk and its compacted part), so that two different chunks
// of the same window never share a quarantine key.
func (s *Store) quarantineKey(shard int, member string, raw, compacted []byte) string {
	d := xxhash.New()
	_, _ = d.Write(binary.BigEndian.AppendUint64(nil, uint64(len(raw))))
	_, _ = d.Write(raw)
	_, _ = d.Write(compacted)
	return fmt.Sprintf("%s:q:%s:%016x", s.tag(shard), member, d.Sum64())
}

// quarantinedCompactedKey is where the compacted part of a quarantined chunk
// is kept: the quarantine key with "qz" for "q".
func quarantinedCompactedKey(qkey string) string {
	return strings.Replace(qkey, "}:q:", "}:qz:", 1)
}

func (s *Store) quarantineIndexKey(shard int) string { return s.tag(shard) + ":qidx" }

func member(id model.SeriesID, start int64) string {
	return id.String() + ":" + strconv.FormatInt(start, 10)
}

func parseMember(m string) (model.SeriesID, int64, error) {
	idHex, startStr, ok := strings.Cut(m, ":")
	if !ok {
		return 0, 0, fmt.Errorf("invalid chunk member %q", m)
	}
	id, err := strconv.ParseUint(idHex, 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid chunk member %q: %w", m, err)
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid chunk member %q: %w", m, err)
	}
	return model.SeriesID(id), start, nil
}

// Shards returns the number of shards.
func (s *Store) Shards() int { return s.layout.Shards }

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }

// Close closes the client.
func (s *Store) Close() error { return s.client.Close() }

type pendingChunk struct {
	id         model.SeriesID
	start      int64
	member     string
	due        int64
	data       []byte
	samples    int
	histograms int
}

type scriptCall struct {
	keys       []string
	args       []any
	samples    []int // samples per chunk, to count rejections
	histograms []int // histograms per chunk
}

// Append stores samples in their chunk windows.
func (s *Store) Append(ctx context.Context, batch []storage.SeriesSamples) (storage.AppendResult, error) {
	shards := make(map[int]map[string]*pendingChunk)
	for _, ss := range batch {
		shard := s.layout.Shard(ss.ID)
		chunks := shards[shard]
		if chunks == nil {
			chunks = make(map[string]*pendingChunk)
			shards[shard] = chunks
		}
		for _, smp := range ss.Samples {
			start := s.layout.WindowStart(ss.ID, smp.T)
			m := member(ss.ID, start)
			c := chunks[m]
			if c == nil {
				c = &pendingChunk{id: ss.ID, start: start, member: m, due: s.layout.Due(start)}
				chunks[m] = c
			}
			c.data = chunk.AppendRaw(c.data, []model.Sample{smp})
			c.samples++
			if smp.IsHistogram() {
				c.histograms++
			}
		}
	}

	var calls []scriptCall
	for shard, chunks := range shards {
		members := make([]string, 0, len(chunks))
		for m := range chunks {
			members = append(members, m)
		}
		slices.Sort(members)
		for len(members) > 0 {
			n := min(len(members), maxChunksPerCall)
			call := scriptCall{
				keys:    make([]string, 0, 2*n+3),
				args:    make([]any, 0, 4*n+1),
				samples: make([]int, 0, n),
			}
			call.keys = append(call.keys, s.dueKey(shard), s.inflightKey(shard), s.clockKey(shard))
			call.args = append(call.args, s.maxChunkBytes)
			for _, m := range members[:n] {
				c := chunks[m]
				call.keys = append(call.keys, s.chunkKey(shard, m), s.windowsKey(shard, c.id))
				call.args = append(call.args, m, c.start, c.due, c.data)
				call.samples = append(call.samples, c.samples)
				call.histograms = append(call.histograms, c.histograms)
			}
			calls = append(calls, call)
			members = members[n:]
		}
	}

	results, err := s.runScripts(ctx, appendScript, calls)
	if err != nil {
		return storage.AppendResult{}, err
	}
	var res storage.AppendResult
	for i, call := range calls {
		reply, err := results[i].Slice()
		if err != nil || len(reply) != 2 {
			return storage.AppendResult{}, fmt.Errorf("append: unexpected reply %v: %w", reply, err)
		}
		accepted := make([]bool, len(call.samples))
		for i := range accepted {
			accepted[i] = true
		}
		count := func(v any) int {
			idxs, _ := v.([]any)
			n := 0
			for _, x := range idxs {
				if idx, ok := x.(int64); ok && idx >= 0 && int(idx) < len(call.samples) && accepted[idx] {
					n += call.samples[idx]
					accepted[idx] = false
				}
			}
			return n
		}
		res.TooOld += count(reply[0])
		res.ChunkFull += count(reply[1])
		for i, ok := range accepted {
			if ok {
				res.Accepted += call.samples[i]
				res.AcceptedHistograms += call.histograms[i]
			}
		}
	}
	return res, nil
}

// runScripts executes script calls in pipelines and transparently reloads
// the script on nodes that lost their script cache (restart, failover,
// SCRIPT FLUSH or a newly added cluster node).
func (s *Store) runScripts(ctx context.Context, script *redis.Script, calls []scriptCall) ([]*redis.Cmd, error) {
	results := make([]*redis.Cmd, len(calls))
	for start := 0; start < len(calls); start += maxPipelineCmds {
		end := min(start+maxPipelineCmds, len(calls))
		pipe := s.client.Pipeline()
		for i := start; i < end; i++ {
			results[i] = script.EvalSha(ctx, pipe, calls[i].keys, calls[i].args...)
		}
		_, _ = pipe.Exec(ctx) // errors are inspected per command below

		var retry []int
		for i := start; i < end; i++ {
			if err := results[i].Err(); err != nil {
				if !redis.HasErrorPrefix(err, "NOSCRIPT") {
					return nil, fmt.Errorf("redis script: %w", err)
				}
				retry = append(retry, i)
			}
		}
		if len(retry) == 0 {
			continue
		}
		pipe = s.client.Pipeline()
		for _, i := range retry {
			results[i] = script.Eval(ctx, pipe, calls[i].keys, calls[i].args...)
		}
		_, _ = pipe.Exec(ctx)
		for _, i := range retry {
			if err := results[i].Err(); err != nil {
				return nil, fmt.Errorf("redis script: %w", err)
			}
		}
	}
	return results, nil
}

// Claim leases up to limit closed chunks of a shard.
func (s *Store) Claim(ctx context.Context, shard, limit int) ([]storage.ChunkRef, error) {
	members, err := claimScript.Run(ctx, s.client,
		[]string{s.dueKey(shard), s.inflightKey(shard), s.inflightDueKey(shard)},
		limit, s.lease.Milliseconds(),
	).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("claim shard %d: %w", shard, err)
	}
	refs := make([]storage.ChunkRef, 0, len(members))
	for _, m := range members {
		id, start, err := parseMember(m)
		if err != nil {
			s.logger.Error("dropping invalid in-flight member", "shard", shard, "member", m, "err", err)
			_ = s.client.ZRem(ctx, s.inflightKey(shard), m).Err()
			_ = s.client.ZRem(ctx, s.inflightDueKey(shard), m).Err()
			continue
		}
		refs = append(refs, storage.ChunkRef{Shard: shard, Series: id, Start: start})
	}
	return refs, nil
}

// Load reads claimed chunks. Undecodable chunks are quarantined and skipped.
func (s *Store) Load(ctx context.Context, refs []storage.ChunkRef) (storage.LoadResult, error) {
	cmds := make([]*redis.SliceCmd, len(refs))
	if err := s.pipelined(ctx, len(refs), func(pipe redis.Pipeliner, i int) {
		m := member(refs[i].Series, refs[i].Start)
		cmds[i] = pipe.MGet(ctx, s.chunkKey(refs[i].Shard, m), s.compactedKey(refs[i].Shard, m))
	}); err != nil {
		return storage.LoadResult{}, err
	}
	res := storage.LoadResult{Chunks: make([]storage.HotChunk, 0, len(refs))}
	for i, cmd := range cmds {
		raw, compacted, ok, err := chunkParts(cmd)
		if err != nil {
			return storage.LoadResult{}, fmt.Errorf("load chunk: %w", err)
		}
		if !ok {
			res.Missing = append(res.Missing, refs[i])
			continue
		}
		frag, samples, records, err := chunk.DecodeHot(raw, compacted)
		if err != nil {
			if s.quarantine(ctx, refs[i], raw, compacted, err) {
				res.Quarantined++
			}
			continue
		}
		res.Chunks = append(res.Chunks, storage.HotChunk{
			Ref:           refs[i],
			Fragment:      frag,
			Size:          len(raw),
			CompactedSize: len(compacted),
			Records:       records,
			Samples:       samples,
		})
	}
	return res, nil
}

// chunkParts returns the chunk and its compacted part from an MGET of both;
// ok is false if the chunk does not exist.
func chunkParts(cmd *redis.SliceCmd) (raw, compacted []byte, ok bool, err error) {
	vals, err := cmd.Result()
	if err != nil {
		return nil, nil, false, err
	}
	if len(vals) != 2 {
		return nil, nil, false, fmt.Errorf("MGET returned %d values", len(vals))
	}
	part := func(v any) ([]byte, error) {
		switch v := v.(type) {
		case nil:
			return nil, nil
		case string:
			return []byte(v), nil
		default:
			return nil, fmt.Errorf("unexpected value of type %T", v)
		}
	}
	if raw, err = part(vals[0]); err != nil || raw == nil {
		return nil, nil, false, err
	}
	if compacted, err = part(vals[1]); err != nil {
		return nil, nil, false, err
	}
	return raw, compacted, true, nil
}

// quarantine moves an undecodable chunk and its compacted part to
// quarantine keys, unless the chunk changed since it was read. It reports
// whether the chunk was moved; otherwise it stays in flight and is retried
// after its lease.
func (s *Store) quarantine(ctx context.Context, ref storage.ChunkRef, raw, compacted []byte, cause error) bool {
	m := member(ref.Series, ref.Start)
	qkey := s.quarantineKey(ref.Shard, m, raw, compacted)
	head := raw[:min(len(raw), chunk.HeaderLen)]
	res, err := quarantineScript.Run(ctx, s.client,
		[]string{s.inflightKey(ref.Shard), s.inflightDueKey(ref.Shard), s.chunkKey(ref.Shard, m), qkey,
			s.quarantineIndexKey(ref.Shard), s.windowsKey(ref.Shard, ref.Series), s.compactedKey(ref.Shard, m),
			quarantinedCompactedKey(qkey)},
		m, head, len(raw), s.quarantineTTL.Milliseconds(), ref.Start, len(compacted),
	).Int()
	switch {
	case err != nil:
		s.logger.Error("quarantine failed; the chunk will be retried after its lease", "shard", ref.Shard, "member", m, "err", err)
		return false
	case res == 0:
		s.logger.Warn("undecodable chunk changed since it was read; it will be retried after its lease",
			"shard", ref.Shard, "member", m, "err", cause)
		return false
	case res < 0:
		return false // gone: completed by another flusher
	}
	s.logger.Error("quarantined undecodable chunk", "shard", ref.Shard, "member", m, "key", qkey,
		"bytes", len(raw), "compacted_bytes", len(compacted), "err", cause)
	return true
}

// Complete deletes chunks that have been persisted, unless they changed
// since they were loaded.
func (s *Store) Complete(ctx context.Context, chunks []storage.HotChunk) error {
	items := make([]finishItem, len(chunks))
	for i, c := range chunks {
		items[i] = finishItem{ref: c.Ref, header: fmt.Sprintf("%016d", c.Fragment), size: c.Size, compactedSize: c.CompactedSize}
	}
	return s.finish(ctx, items)
}

// Release drops the in-flight entries of chunks that no longer exist.
func (s *Store) Release(ctx context.Context, refs []storage.ChunkRef) error {
	items := make([]finishItem, len(refs))
	for i, r := range refs {
		items[i] = finishItem{ref: r}
	}
	return s.finish(ctx, items)
}

type finishItem struct {
	ref           storage.ChunkRef
	header        string // empty: release a missing chunk
	size          int
	compactedSize int
}

func (s *Store) finish(ctx context.Context, items []finishItem) error {
	byShard := make(map[int][]finishItem)
	for _, it := range items {
		byShard[it.ref.Shard] = append(byShard[it.ref.Shard], it)
	}
	var calls []scriptCall
	for shard, its := range byShard {
		for len(its) > 0 {
			n := min(len(its), maxChunksPerCall)
			call := scriptCall{keys: []string{s.inflightKey(shard), s.inflightDueKey(shard)}}
			for _, it := range its[:n] {
				m := member(it.ref.Series, it.ref.Start)
				call.keys = append(call.keys, s.chunkKey(shard, m), s.compactedKey(shard, m), s.windowsKey(shard, it.ref.Series))
				call.args = append(call.args, m, it.ref.Start, it.header, it.size, it.compactedSize)
			}
			calls = append(calls, call)
			its = its[n:]
		}
	}
	_, err := s.runScripts(ctx, completeScript, calls)
	return err
}

// Read returns the samples of the given series within [mint, maxt] that are
// currently held in Redis.
//
// It first reads the window index of every series, then only the chunks
// that exist, so that it issues one command per series plus one per chunk,
// whatever the length of [mint, maxt] and the chunk range.
func (s *Store) Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	if maxt < mint {
		return map[model.SeriesID][]model.Sample{}, nil
	}
	windows := make([]*redis.StringSliceCmd, len(ids))
	if err := s.pipelined(ctx, len(ids), func(pipe redis.Pipeliner, i int) {
		id := ids[i]
		windows[i] = pipe.ZRangeByScore(ctx, s.windowsKey(s.layout.Shard(id), id), &redis.ZRangeBy{
			Min: strconv.FormatInt(s.layout.WindowStart(id, mint), 10),
			Max: strconv.FormatInt(s.layout.WindowStart(id, maxt), 10),
		})
	}); err != nil {
		return nil, err
	}
	type lookup struct {
		id     model.SeriesID
		shard  int
		member string
	}
	var lookups []lookup
	for i, id := range ids {
		if err := windows[i].Err(); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("read window index: %w", err)
		}
		shard := s.layout.Shard(id)
		for _, w := range windows[i].Val() {
			start, err := strconv.ParseInt(w, 10, 64)
			if err != nil {
				s.logger.Warn("skipping invalid window index entry", "series", id, "entry", w)
				continue
			}
			lookups = append(lookups, lookup{id: id, shard: shard, member: member(id, start)})
		}
	}
	// Both parts of a chunk are read with one MGET, so that a compaction is
	// never seen half done.
	cmds := make([]*redis.SliceCmd, len(lookups))
	if err := s.pipelined(ctx, len(lookups), func(pipe redis.Pipeliner, i int) {
		l := lookups[i]
		cmds[i] = pipe.MGet(ctx, s.chunkKey(l.shard, l.member), s.compactedKey(l.shard, l.member))
	}); err != nil {
		return nil, err
	}
	out := make(map[model.SeriesID][]model.Sample)
	for i, cmd := range cmds {
		raw, compacted, ok, err := chunkParts(cmd)
		if err != nil {
			return nil, fmt.Errorf("read chunk: %w", err)
		}
		if !ok {
			continue // completed since the window index was read
		}
		_, samples, _, err := chunk.DecodeHot(raw, compacted)
		if err != nil {
			s.logger.Warn("skipping undecodable chunk", "key", s.chunkKey(lookups[i].shard, lookups[i].member), "err", err)
			continue
		}
		id := lookups[i].id
		out[id] = append(out[id], chunk.Filter(samples, mint, maxt)...)
	}
	for id, samples := range out {
		out[id] = chunk.Normalize(samples)
	}
	return out, nil
}

// pipelined issues n commands built by add in bounded pipelines. A missing
// key (redis.Nil) is not an error.
func (s *Store) pipelined(ctx context.Context, n int, add func(pipe redis.Pipeliner, i int)) error {
	for start := 0; start < n; start += maxPipelineCmds {
		end := min(start+maxPipelineCmds, n)
		pipe := s.client.Pipeline()
		for i := start; i < end; i++ {
			add(pipe, i)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("redis pipeline: %w", err)
		}
	}
	return nil
}

// Backlog reports the number of pending and in-flight chunks and the
// oldest chunk across all shards.
func (s *Store) Backlog(ctx context.Context) (storage.Backlog, error) {
	n := s.layout.Shards
	due := make([]*redis.IntCmd, n)
	inf := make([]*redis.IntCmd, n)
	quarantined := make([]*redis.IntCmd, n)
	oldestDue := make([]*redis.ZSliceCmd, n)
	oldestInf := make([]*redis.ZSliceCmd, n)
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	err := s.pipelined(ctx, n, func(pipe redis.Pipeliner, i int) {
		due[i] = pipe.ZCard(ctx, s.dueKey(i))
		inf[i] = pipe.ZCard(ctx, s.inflightKey(i))
		quarantined[i] = pipe.ZCount(ctx, s.quarantineIndexKey(i), "("+now, "+inf")
		oldestDue[i] = pipe.ZRangeWithScores(ctx, s.dueKey(i), 0, 0)
		oldestInf[i] = pipe.ZRangeWithScores(ctx, s.inflightDueKey(i), 0, 0)
	})
	if err != nil {
		return storage.Backlog{}, err
	}
	var b storage.Backlog
	for i := range n {
		b.PendingChunks += due[i].Val()
		b.InflightChunks += inf[i].Val()
		b.QuarantinedChunks += quarantined[i].Val()
		for _, zs := range [][]redis.Z{oldestDue[i].Val(), oldestInf[i].Val()} {
			if len(zs) > 0 {
				if t := int64(zs[0].Score); b.OldestDue == 0 || t < b.OldestDue {
					b.OldestDue = t
				}
			}
		}
	}
	if b.OldestDue != 0 {
		b.OldestStart = b.OldestDue - s.layout.ChunkRange - s.layout.Grace
	}
	return b, nil
}
