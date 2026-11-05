// Package hot implements the in-memory tier on Redis (or Valkey), in
// standalone, Sentinel or Cluster mode.
//
// Key layout for shard N (all keys of a shard share the hash tag {N}):
//
//	<prefix>:{N}:c:<series>:<window>  chunk: 16-byte header + raw samples
//	<prefix>:{N}:due                  sorted set: chunk member -> due time
//	<prefix>:{N}:inf                  sorted set: chunk member -> lease expiry
//	<prefix>:{N}:q:<series>:<window>  quarantined (undecodable) chunk
//	<prefix>:meta                     hash: layout parameters
package hot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

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

// NewClient creates a Redis client from configuration.
func NewClient(cfg config.RedisConfig) (redis.UniversalClient, error) {
	tlsCfg, err := cfg.TLS.Build()
	if err != nil {
		return nil, fmt.Errorf("redis tls: %w", err)
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

// Options configures a Store.
type Options struct {
	Prefix string
	Layout model.Layout
	Lease  time.Duration
	Logger *slog.Logger
}

// Store is a storage.HotStore backed by Redis.
type Store struct {
	client redis.UniversalClient
	prefix string
	layout model.Layout
	lease  time.Duration
	logger *slog.Logger
}

var _ storage.HotStore = (*Store)(nil)

// New creates a Store and checks that the layout matches the one already
// recorded in Redis.
func New(ctx context.Context, client redis.UniversalClient, opts Options) (*Store, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Store{
		client: client,
		prefix: opts.Prefix,
		layout: opts.Layout,
		lease:  opts.Lease,
		logger: opts.Logger,
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	if err := s.checkLayout(ctx); err != nil {
		return nil, err
	}
	s.warnOnEviction(ctx)
	return s, nil
}

func (s *Store) checkLayout(ctx context.Context) error {
	key := s.prefix + ":meta"
	shards := strconv.Itoa(s.layout.Shards)
	if err := s.client.HSetNX(ctx, key, "shards", shards).Err(); err != nil {
		return fmt.Errorf("redis meta: %w", err)
	}
	got, err := s.client.HGet(ctx, key, "shards").Result()
	if err != nil {
		return fmt.Errorf("redis meta: %w", err)
	}
	if got != shards {
		return fmt.Errorf("storage.shards is %s but the data in Redis was written with %s shards; "+
			"changing the number of shards is not supported", shards, got)
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

func (s *Store) dueKey(shard int) string      { return s.tag(shard) + ":due" }
func (s *Store) inflightKey(shard int) string { return s.tag(shard) + ":inf" }
func (s *Store) chunkKey(shard int, member string) string {
	return s.tag(shard) + ":c:" + member
}

func (s *Store) quarantineKey(shard int, member string) string {
	return s.tag(shard) + ":q:" + member
}

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
	member  string
	due     int64
	data    []byte
	samples int
}

type scriptCall struct {
	keys    []string
	args    []any
	samples []int // samples per chunk, to count rejections
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
				c = &pendingChunk{member: m, due: s.layout.Due(start)}
				chunks[m] = c
			}
			c.data = chunk.AppendRaw(c.data, []model.Sample{smp})
			c.samples++
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
				keys:    make([]string, 0, n+2),
				args:    make([]any, 0, 3*n),
				samples: make([]int, 0, n),
			}
			call.keys = append(call.keys, s.dueKey(shard), s.inflightKey(shard))
			for _, m := range members[:n] {
				c := chunks[m]
				call.keys = append(call.keys, s.chunkKey(shard, m))
				call.args = append(call.args, m, c.due, c.data)
				call.samples = append(call.samples, c.samples)
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
		rejected, err := results[i].Int64Slice()
		if err != nil {
			return storage.AppendResult{}, fmt.Errorf("append: unexpected reply: %w", err)
		}
		total := 0
		for _, n := range call.samples {
			total += n
		}
		tooOld := 0
		for _, idx := range rejected {
			if idx >= 0 && int(idx) < len(call.samples) {
				tooOld += call.samples[idx]
			}
		}
		res.TooOld += tooOld
		res.Accepted += total - tooOld
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
		[]string{s.dueKey(shard), s.inflightKey(shard)},
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
			continue
		}
		refs = append(refs, storage.ChunkRef{Shard: shard, Series: id, Start: start})
	}
	return refs, nil
}

// Load reads claimed chunks. Undecodable chunks are quarantined and skipped.
func (s *Store) Load(ctx context.Context, refs []storage.ChunkRef) ([]storage.HotChunk, error) {
	cmds := make([]*redis.StringCmd, len(refs))
	if err := s.pipelined(ctx, len(refs), func(pipe redis.Pipeliner, i int) {
		cmds[i] = pipe.Get(ctx, s.chunkKey(refs[i].Shard, member(refs[i].Series, refs[i].Start)))
	}); err != nil {
		return nil, err
	}
	out := make([]storage.HotChunk, 0, len(refs))
	for i, cmd := range cmds {
		b, err := cmd.Bytes()
		if errors.Is(err, redis.Nil) {
			continue // already completed by another flusher
		}
		if err != nil {
			return nil, fmt.Errorf("load chunk: %w", err)
		}
		frag, samples, err := chunk.DecodeHot(b)
		if err != nil {
			s.quarantine(ctx, refs[i], err)
			continue
		}
		out = append(out, storage.HotChunk{Ref: refs[i], Fragment: frag, Samples: samples})
	}
	return out, nil
}

func (s *Store) quarantine(ctx context.Context, ref storage.ChunkRef, cause error) {
	m := member(ref.Series, ref.Start)
	s.logger.Error("quarantining undecodable chunk", "shard", ref.Shard, "member", m, "err", cause)
	err := quarantineScript.Run(ctx, s.client,
		[]string{s.inflightKey(ref.Shard), s.chunkKey(ref.Shard, m), s.quarantineKey(ref.Shard, m)},
		m,
	).Err()
	if err != nil {
		s.logger.Error("quarantine failed", "member", m, "err", err)
	}
}

// Complete deletes chunks that have been persisted.
func (s *Store) Complete(ctx context.Context, chunks []storage.HotChunk) error {
	byShard := make(map[int][]storage.HotChunk)
	for _, c := range chunks {
		byShard[c.Ref.Shard] = append(byShard[c.Ref.Shard], c)
	}
	var calls []scriptCall
	for shard, cs := range byShard {
		for len(cs) > 0 {
			n := min(len(cs), maxChunksPerCall)
			call := scriptCall{keys: []string{s.inflightKey(shard)}}
			for _, c := range cs[:n] {
				m := member(c.Ref.Series, c.Ref.Start)
				call.keys = append(call.keys, s.chunkKey(shard, m))
				call.args = append(call.args, m, fmt.Sprintf("%016d", c.Fragment))
			}
			calls = append(calls, call)
			cs = cs[n:]
		}
	}
	_, err := s.runScripts(ctx, completeScript, calls)
	return err
}

// Read returns the samples of the given series within [mint, maxt] that are
// currently held in Redis.
func (s *Store) Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	type lookup struct {
		id  model.SeriesID
		key string
	}
	var lookups []lookup
	for _, id := range ids {
		shard := s.layout.Shard(id)
		for _, w := range s.layout.Windows(id, mint, maxt) {
			lookups = append(lookups, lookup{id: id, key: s.chunkKey(shard, member(id, w))})
		}
	}
	cmds := make([]*redis.StringCmd, len(lookups))
	if err := s.pipelined(ctx, len(lookups), func(pipe redis.Pipeliner, i int) {
		cmds[i] = pipe.Get(ctx, lookups[i].key)
	}); err != nil {
		return nil, err
	}
	out := make(map[model.SeriesID][]model.Sample)
	for i, cmd := range cmds {
		b, err := cmd.Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read chunk: %w", err)
		}
		_, samples, err := chunk.DecodeHot(b)
		if err != nil {
			s.logger.Warn("skipping undecodable chunk", "key", lookups[i].key, "err", err)
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
// earliest due time across all shards.
func (s *Store) Backlog(ctx context.Context) (storage.Backlog, error) {
	n := s.layout.Shards
	due := make([]*redis.IntCmd, n)
	inf := make([]*redis.IntCmd, n)
	oldest := make([]*redis.ZSliceCmd, n)
	err := s.pipelined(ctx, n, func(pipe redis.Pipeliner, i int) {
		due[i] = pipe.ZCard(ctx, s.dueKey(i))
		inf[i] = pipe.ZCard(ctx, s.inflightKey(i))
		oldest[i] = pipe.ZRangeWithScores(ctx, s.dueKey(i), 0, 0)
	})
	if err != nil {
		return storage.Backlog{}, err
	}
	var b storage.Backlog
	for i := range n {
		b.PendingChunks += due[i].Val()
		b.InflightChunks += inf[i].Val()
		if zs := oldest[i].Val(); len(zs) > 0 {
			if t := int64(zs[0].Score); b.OldestDue == 0 || t < b.OldestDue {
				b.OldestDue = t
			}
		}
	}
	return b, nil
}
