package hot

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Compaction moves the raw records of open chunks into their compacted
// part, encoded like the persistent tier, so that they take a fraction of
// the memory until they are flushed (see docs/architecture.md, Compaction).
// The encoding work is done here, outside Redis; Redis only checks that the
// chunk did not change and swaps the parts.

const (
	// compactPage is how many open chunks are listed at a time.
	compactPage = 1000
	// compactCallBytes bounds the arguments of one compaction script call.
	compactCallBytes = 8 << 20
)

// Compact implements storage.HotStore.
func (s *Store) Compact(ctx context.Context, shard int, interval time.Duration) (storage.CompactResult, error) {
	var res storage.CompactResult
	err := compactLeaseScript.Run(ctx, s.client, []string{s.compactLeaseKey(shard)},
		max(interval.Milliseconds(), 1)).Err()
	if errors.Is(err, redis.Nil) {
		res.Skipped = true
		return res, nil
	}
	if err != nil {
		return res, fmt.Errorf("compaction lease of shard %d: %w", shard, err)
	}
	// Chunks closing soon are left to the flusher. The script checks the due
	// time again with the clock of Redis.
	minDue := strconv.FormatInt(time.Now().Add(s.compactMargin).UnixMilli(), 10)
	for offset := int64(0); ; offset += compactPage {
		members, err := s.client.ZRangeByScore(ctx, s.dueKey(shard), &redis.ZRangeBy{
			Min: "(" + minDue, Max: "+inf", Offset: offset, Count: compactPage,
		}).Result()
		if err != nil {
			return res, fmt.Errorf("list open chunks of shard %d: %w", shard, err)
		}
		if err := s.compactMembers(ctx, shard, members, &res); err != nil {
			return res, err
		}
		if len(members) < compactPage {
			return res, nil
		}
	}
}

// compactMembers compacts the chunks among members that have raw records.
func (s *Store) compactMembers(ctx context.Context, shard int, members []string, res *storage.CompactResult) error {
	lengths := make([]*redis.IntCmd, len(members))
	if err := s.pipelined(ctx, len(members), func(pipe redis.Pipeliner, i int) {
		lengths[i] = pipe.StrLen(ctx, s.chunkKey(shard, members[i]))
	}); err != nil {
		return err
	}
	var todo []string
	for i, m := range members {
		if lengths[i].Val() > chunk.HeaderLen {
			todo = append(todo, m)
		}
	}
	for len(todo) > 0 {
		batch := todo[:min(len(todo), maxChunksPerCall)]
		todo = todo[len(batch):]
		if err := s.compactBatch(ctx, shard, batch, res); err != nil {
			return err
		}
	}
	return nil
}

// compactSizes are the sizes replaced and written by the compaction of one
// chunk.
type compactSizes struct{ before, after int }

// compactBatch reads, encodes and swaps the parts of up to maxChunksPerCall
// chunks.
func (s *Store) compactBatch(ctx context.Context, shard int, members []string, res *storage.CompactResult) error {
	cmds := make([]*redis.SliceCmd, len(members))
	if err := s.pipelined(ctx, len(members), func(pipe redis.Pipeliner, i int) {
		cmds[i] = pipe.MGet(ctx, s.chunkKey(shard, members[i]), s.compactedKey(shard, members[i]))
	}); err != nil {
		return err
	}
	var (
		calls []scriptCall
		sizes [][]compactSizes
		call  scriptCall
		cur   []compactSizes
		bytes int
	)
	// As many records as the raw part may hold.
	limit := (s.maxChunkBytes - chunk.HeaderLen) / chunk.RecordLen
	for i, cmd := range cmds {
		raw, compacted, ok, err := chunkParts(cmd)
		if err != nil {
			return fmt.Errorf("read chunk: %w", err)
		}
		if !ok || len(raw) <= chunk.HeaderLen {
			continue // completed, or compacted by someone else, since it was listed
		}
		_, samples, records, err := chunk.DecodeHot(raw, compacted)
		if err == nil && records > limit {
			// Further records stay raw, up to the size limit of the raw
			// part: a chunk holds at most twice the records of a full raw
			// part.
			continue
		}
		var next []byte
		if err == nil {
			next, err = chunk.EncodeCompacted(raw[:chunk.HeaderLen], records, samples)
		}
		if err != nil {
			res.Failed++
			s.logger.Warn("cannot compact chunk; it is left as it is", "shard", shard, "member", members[i], "err", err)
			continue
		}
		n := len(raw) + len(compacted) + len(next)
		if len(cur) > 0 && bytes+n > compactCallBytes {
			calls, sizes = append(calls, call), append(sizes, cur)
			call, cur, bytes = scriptCall{}, nil, 0
		}
		if len(cur) == 0 {
			call.keys = []string{s.dueKey(shard)}
		}
		call.keys = append(call.keys, s.chunkKey(shard, members[i]), s.compactedKey(shard, members[i]))
		call.args = append(call.args, members[i], raw, compacted, next)
		cur = append(cur, compactSizes{before: len(raw) - chunk.HeaderLen + len(compacted), after: len(next)})
		bytes += n
	}
	if len(cur) > 0 {
		calls, sizes = append(calls, call), append(sizes, cur)
	}
	if len(calls) == 0 {
		return nil
	}
	if s.beforeCompact != nil {
		s.beforeCompact()
	}
	replies, err := s.runScripts(ctx, compactScript, calls)
	if err != nil {
		return fmt.Errorf("compact shard %d: %w", shard, err)
	}
	for i, reply := range replies {
		idxs, err := reply.Int64Slice()
		if err != nil {
			return fmt.Errorf("compact shard %d: unexpected reply: %w", shard, err)
		}
		done := make([]bool, len(sizes[i]))
		for _, idx := range idxs {
			if idx >= 0 && int(idx) < len(done) {
				done[idx] = true
			}
		}
		for j, ok := range done {
			if !ok {
				res.Changed++
				continue
			}
			res.Compacted++
			res.BytesBefore += int64(sizes[i][j].before)
			res.BytesAfter += int64(sizes[i][j].after)
		}
	}
	return nil
}
