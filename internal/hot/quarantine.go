package hot

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/model"
)

// QuarantinedChunk describes an undecodable chunk kept for inspection.
type QuarantinedChunk struct {
	Key    string
	Shard  int
	Series model.SeriesID
	Start  int64 // window start in milliseconds
	Bytes  int64
	// Header is the first 16 bytes as stored. In a healthy chunk it is the
	// creation time of the chunk in microseconds.
	Header string
	// Expires is when Redis deletes the chunk; zero if it is kept until it
	// is deleted explicitly.
	Expires time.Time
}

// ListQuarantined returns the quarantined chunks of every shard. Index
// entries whose chunk no longer exists (deleted outside of xtsdb) are
// removed.
func (s *Store) ListQuarantined(ctx context.Context) ([]QuarantinedChunk, error) {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	n := s.layout.Shards
	ranges := make([]*redis.ZSliceCmd, n)
	if err := s.pipelined(ctx, n, func(pipe redis.Pipeliner, i int) {
		ranges[i] = pipe.ZRangeByScoreWithScores(ctx, s.quarantineIndexKey(i), &redis.ZRangeBy{Min: "(" + now, Max: "+inf"})
	}); err != nil {
		return nil, err
	}
	var out []QuarantinedChunk
	for shard, cmd := range ranges {
		for _, z := range cmd.Val() {
			key, _ := z.Member.(string)
			c := QuarantinedChunk{Key: key, Shard: shard}
			if !math.IsInf(z.Score, 1) {
				c.Expires = time.UnixMilli(int64(z.Score))
			}
			if series, start, ok := s.parseQuarantineKey(key); ok {
				c.Series, c.Start = series, start
			}
			out = append(out, c)
		}
	}

	sizes := make([]*redis.IntCmd, len(out))
	heads := make([]*redis.StringCmd, len(out))
	if err := s.pipelined(ctx, len(out), func(pipe redis.Pipeliner, i int) {
		sizes[i] = pipe.StrLen(ctx, out[i].Key)
		heads[i] = pipe.GetRange(ctx, out[i].Key, 0, 15)
	}); err != nil {
		return nil, err
	}
	var gone []QuarantinedChunk
	live := out[:0]
	for i, c := range out {
		c.Bytes = sizes[i].Val()
		c.Header = heads[i].Val()
		if c.Bytes == 0 {
			gone = append(gone, c)
			continue
		}
		live = append(live, c)
	}
	if err := s.pipelined(ctx, len(gone), func(pipe redis.Pipeliner, i int) {
		pipe.ZRem(ctx, s.quarantineIndexKey(gone[i].Shard), gone[i].Key)
	}); err != nil {
		return nil, err
	}
	return live, nil
}

// DeleteQuarantined deletes quarantined chunks by key and returns how many
// existed. Keys that are not quarantine keys of this store are refused.
func (s *Store) DeleteQuarantined(ctx context.Context, keys []string) (int, error) {
	shards := make([]int, len(keys))
	for i, k := range keys {
		shard, ok := s.quarantineShard(k)
		if !ok {
			return 0, fmt.Errorf("%q is not a quarantine key of prefix %q", k, s.prefix)
		}
		shards[i] = shard
	}
	dels := make([]*redis.IntCmd, len(keys))
	if err := s.pipelined(ctx, len(keys), func(pipe redis.Pipeliner, i int) {
		dels[i] = pipe.Del(ctx, keys[i])
		pipe.ZRem(ctx, s.quarantineIndexKey(shards[i]), keys[i])
	}); err != nil {
		return 0, err
	}
	n := 0
	for _, d := range dels {
		n += int(d.Val())
	}
	return n, nil
}

// quarantineShard returns the shard of a quarantine key.
func (s *Store) quarantineShard(key string) (int, bool) {
	rest, ok := strings.CutPrefix(key, s.prefix+":{")
	if !ok {
		return 0, false
	}
	num, rest, ok := strings.Cut(rest, "}:q:")
	if !ok || rest == "" {
		return 0, false
	}
	shard, err := strconv.Atoi(num)
	if err != nil || shard < 0 || shard >= s.layout.Shards || strconv.Itoa(shard) != num {
		return 0, false
	}
	return shard, true
}

// parseQuarantineKey extracts the series and window start from
// <prefix>:{N}:q:<series>:<window>[:<digest>].
func (s *Store) parseQuarantineKey(key string) (model.SeriesID, int64, bool) {
	_, rest, ok := strings.Cut(key, "}:q:")
	if !ok {
		return 0, 0, false
	}
	parts := strings.Split(rest, ":")
	if len(parts) < 2 {
		return 0, 0, false
	}
	id, start, err := parseMember(parts[0] + ":" + parts[1])
	if err != nil {
		return 0, 0, false
	}
	return id, start, true
}
