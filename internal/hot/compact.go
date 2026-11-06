package hot

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Compaction moves the raw float records of open chunks into their
// compacted part, XOR-encoded like the persistent tier, so that they take a
// fraction of the memory until they are flushed (see docs/architecture.md,
// Compaction). The encoding work is done here, outside Redis; Redis only
// checks that the chunk did not change and swaps the parts.

// compactCallBytes bounds the arguments of one compaction script call.
const compactCallBytes = 8 << 20

// Compact implements storage.HotStore.
//
// A pass stops once it has run for an interval, so that passes over a large
// shard do not pile up; the rest of the shard is compacted at the next one.
func (s *Store) Compact(ctx context.Context, shard int, interval time.Duration) (storage.CompactResult, error) {
	var res storage.CompactResult
	start := time.Now()
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
	minDue := "(" + strconv.FormatInt(start.Add(s.compactMargin).UnixMilli(), 10)
	// Pages follow the scores: an offset would make Redis walk the set from
	// its start for every page. skip counts the members of the last score
	// that were already listed.
	skip := 0
	for {
		page, err := s.client.ZRangeByScoreWithScores(ctx, s.dueKey(shard), &redis.ZRangeBy{
			Min: minDue, Max: "+inf", Offset: int64(skip), Count: int64(s.compactPage),
		}).Result()
		if err != nil {
			return res, fmt.Errorf("list open chunks of shard %d: %w", shard, err)
		}
		members := make([]string, len(page))
		for i, z := range page {
			members[i], _ = z.Member.(string)
		}
		if err := s.compactMembers(ctx, shard, members, &res); err != nil {
			return res, err
		}
		if len(page) < s.compactPage || time.Since(start) >= interval {
			return res, nil
		}
		last := strconv.FormatFloat(page[len(page)-1].Score, 'f', -1, 64)
		if last != minDue {
			minDue, skip = last, 0
		}
		for i := len(page) - 1; i >= 0 && page[i].Score == page[len(page)-1].Score; i-- {
			skip++
		}
	}
}

// compactMembers compacts the chunks among members that have enough raw
// records.
func (s *Store) compactMembers(ctx context.Context, shard int, members []string, res *storage.CompactResult) error {
	lengths := make([]*redis.IntCmd, len(members))
	if err := s.pipelined(ctx, len(members), func(pipe redis.Pipeliner, i int) {
		lengths[i] = pipe.StrLen(ctx, s.chunkKey(shard, members[i]))
	}); err != nil {
		return err
	}
	var todo []string
	for i, m := range members {
		if lengths[i].Val() >= int64(chunk.HeaderLen+s.compactMinRaw) {
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
		if !ok || len(raw) < chunk.HeaderLen+s.compactMinRaw {
			continue // completed, or compacted by someone else, since it was listed
		}
		_, samples, records, err := chunk.DecodeHot(raw, compacted)
		if err != nil {
			res.Failed++
			s.logger.Warn("cannot compact undecodable chunk; it is left as it is", "shard", shard, "member", members[i], "err", err)
			continue
		}
		// Chunks with native histograms stay raw (see EncodeCompacted).
		// Beyond the record limit, further records stay raw up to the size
		// limit of the raw part: a chunk holds at most twice the records of
		// a full raw part.
		if records > limit || slices.ContainsFunc(samples, model.Sample.IsHistogram) {
			res.Kept++
			continue
		}
		next, err := chunk.EncodeCompacted(raw[:chunk.HeaderLen], records, samples)
		if err != nil {
			res.Failed++
			s.logger.Warn("cannot compact chunk; it is left as it is", "shard", shard, "member", members[i], "err", err)
			continue
		}
		before := len(raw) - chunk.HeaderLen + len(compacted)
		if len(next) >= before {
			res.Kept++ // incompressible: compaction would take more memory
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
		cur = append(cur, compactSizes{before: before, after: len(next)})
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
