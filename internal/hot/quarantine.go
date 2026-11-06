package hot

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
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
	// Bytes is the size of the chunk and of its compacted part, kept in
	// <prefix>:{N}:qz:... next to the quarantine key.
	Bytes int64
	// Header is the first 16 bytes as stored. In a healthy chunk it is the
	// fragment: the time the chunk was created in microseconds, from the
	// shard's hybrid clock.
	Header string
	// Expires is when Redis deletes the chunk; zero if it is kept until it
	// is deleted explicitly.
	Expires time.Time
}

// ListQuarantined returns the quarantined chunks of every shard: those in
// the quarantine index, and those found by scanning for quarantine keys
// that are not in it (quarantined by earlier builds, which named them
// <prefix>:{N}:q:<series>:<window>). Index entries whose chunk no longer
// exists (deleted outside of xtsdb) are removed.
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
	indexed := make(map[string]bool)
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
			indexed[key] = true
		}
	}
	scanned, err := s.scanQuarantineKeys(ctx)
	if err != nil {
		return nil, err
	}
	nIndexed := len(out)
	for _, key := range scanned {
		shard, ok := s.quarantineShard(key)
		if !ok || indexed[key] {
			continue
		}
		c := QuarantinedChunk{Key: key, Shard: shard}
		if series, start, ok := s.parseQuarantineKey(key); ok {
			c.Series, c.Start = series, start
		}
		out = append(out, c)
	}

	sizes := make([]*redis.IntCmd, len(out))
	compactedSizes := make([]*redis.IntCmd, len(out))
	heads := make([]*redis.StringCmd, len(out))
	ttls := make([]*redis.DurationCmd, len(out))
	if err := s.pipelined(ctx, len(out), func(pipe redis.Pipeliner, i int) {
		sizes[i] = pipe.StrLen(ctx, out[i].Key)
		compactedSizes[i] = pipe.StrLen(ctx, quarantinedCompactedKey(out[i].Key))
		heads[i] = pipe.GetRange(ctx, out[i].Key, 0, 15)
		if i >= nIndexed {
			ttls[i] = pipe.PTTL(ctx, out[i].Key)
		}
	}); err != nil {
		return nil, err
	}
	var gone []QuarantinedChunk
	live := out[:0]
	for i, c := range out {
		c.Header = heads[i].Val()
		if ttls[i] != nil && ttls[i].Val() > 0 {
			c.Expires = time.Now().Add(ttls[i].Val())
		}
		if sizes[i].Val() == 0 {
			if i < nIndexed {
				gone = append(gone, c)
			}
			continue
		}
		c.Bytes = sizes[i].Val() + compactedSizes[i].Val()
		live = append(live, c)
	}
	if err := s.pipelined(ctx, len(gone), func(pipe redis.Pipeliner, i int) {
		pipe.ZRem(ctx, s.quarantineIndexKey(gone[i].Shard), gone[i].Key)
	}); err != nil {
		return nil, err
	}
	return live, nil
}

// scanQuarantineKeys returns every quarantine key of the store, on every
// node of a cluster. It is meant for maintenance commands.
func (s *Store) scanQuarantineKeys(ctx context.Context) ([]string, error) {
	pattern := globEscape(s.prefix) + ":{*}:q:*"
	var (
		mu   sync.Mutex
		keys []string
	)
	scan := func(ctx context.Context, c *redis.Client) error {
		iter := c.Scan(ctx, 0, pattern, 1000).Iterator()
		for iter.Next(ctx) {
			mu.Lock()
			keys = append(keys, iter.Val())
			mu.Unlock()
		}
		return iter.Err()
	}
	var err error
	switch c := s.client.(type) {
	case *redis.ClusterClient:
		err = c.ForEachMaster(ctx, scan)
	case *redis.Client:
		err = scan(ctx, c)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan quarantine keys: %w", err)
	}
	return keys, nil
}

// globEscape escapes the characters that are special in Redis key patterns.
func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(`*?[]\`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// DeleteQuarantined deletes quarantined chunks by key, with their compacted
// parts, and returns how many existed. Keys that are not quarantine keys of
// this store are refused.
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
		pipe.Del(ctx, quarantinedCompactedKey(keys[i]))
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
