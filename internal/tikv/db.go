// Package tikv implements the persistent tier and the series index on the
// RawKV API of TiKV.
//
// TiKV must run with storage.api-version = 2 (or, with api_version v1ttl,
// storage.enable-ttl = true): entries expire through per-key TTLs, like the
// Cassandra backend's rows. Every client of the data must use the atomic
// mode of RawKV, which compare-and-swap needs; xtsdb always does.
package tikv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/pd/client/opt"

	"github.com/yuuki/xtsdb/internal/config"
)

// rawClient is the part of *rawkv.Client that the stores use.
type rawClient interface {
	Get(ctx context.Context, key []byte, options ...rawkv.RawOption) ([]byte, error)
	BatchGet(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) ([][]byte, error)
	PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, options ...rawkv.RawOption) error
	BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...rawkv.RawOption) error
	Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) ([][]byte, [][]byte, error)
	CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, options ...rawkv.RawOption) ([]byte, bool, error)
	DeleteRange(ctx context.Context, startKey, endKey []byte, options ...rawkv.RawOption) error
	Close() error
}

var _ rawClient = (*rawkv.Client)(nil)

const (
	// scanBudget bounds the bytes a single read asks for, below the 10 MiB
	// that TiKV sends at most in one response by default.
	scanBudget = 8 << 20
	// maxScanLimit is the most keys a scan may ask for.
	maxScanLimit = 10240
	// pairOverhead is an estimate of the protobuf framing of a key-value
	// pair in a response.
	pairOverhead = 16
)

// DB is a namespace of a TiKV cluster: every key it reads or writes starts
// with a prefix (storage.key_prefix).
type DB struct {
	raw     rawClient
	keys    keys
	timeout time.Duration
	// budget is scanBudget; tests lower it to exercise reads in several
	// requests.
	budget int
	stop   context.CancelFunc // ends the client's background work
}

// Open connects to the PD servers of a TiKV cluster.
func Open(ctx context.Context, cfg config.TiKVConfig, prefix string, logger *slog.Logger) (*DB, error) {
	redirectLogs(logger)
	opts := []rawkv.ClientOpt{
		rawkv.WithPDOptions(opt.WithCustomTimeoutOption(cfg.Timeout.D()), opt.WithInitMetricsOption(false)),
	}
	switch strings.ToLower(cfg.APIVersion) {
	case "v2", "":
		opts = append(opts, rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2), rawkv.WithKeyspace(cfg.Keyspace))
	case "v1ttl":
		opts = append(opts, rawkv.WithAPIVersion(kvrpcpb.APIVersion_V1TTL))
	default:
		return nil, fmt.Errorf("unknown tikv.api_version %q", cfg.APIVersion)
	}
	if cfg.TLS.Enabled {
		opts = append(opts, rawkv.WithSecurity(tikvconfig.Security{
			ClusterSSLCA:   cfg.TLS.CAFile,
			ClusterSSLCert: cfg.TLS.CertFile,
			ClusterSSLKey:  cfg.TLS.KeyFile,
		}))
	}
	// The PD client runs its background work under the context it is
	// created with, so it gets one of its own that ends with Close, not
	// with ctx. It retries for a long time when the servers are
	// unreachable, so connect in the background and wait at most
	// connect_timeout.
	life, stop := context.WithCancel(context.WithoutCancel(ctx))
	type result struct {
		c   *rawkv.Client
		err error
	}
	done := make(chan result, 1)
	go func() {
		c, err := rawkv.NewClientWithOpts(life, cfg.PDEndpoints, opts...)
		done <- result{c, err}
	}()
	timer := time.NewTimer(cfg.ConnectTimeout.D())
	defer timer.Stop()
	var (
		r        result
		finished bool
	)
	select {
	case r = <-done:
		finished = true
	case <-timer.C:
		r.err = errors.New("timed out")
	case <-ctx.Done():
		r.err = ctx.Err()
	}
	if r.err != nil {
		stop()
		if !finished {
			// Still connecting: close the client if it ever connects.
			go func() {
				if r := <-done; r.c != nil {
					_ = r.c.Close()
				}
			}()
		}
		return nil, fmt.Errorf("connect to tikv (pd %s): %w", strings.Join(cfg.PDEndpoints, ","), r.err)
	}
	r.c.SetAtomicForCAS(true)
	db := newDB(r.c, prefix, cfg.Timeout.D())
	db.stop = stop
	return db, nil
}

func newDB(raw rawClient, prefix string, timeout time.Duration) *DB {
	return &DB{raw: raw, keys: newKeys(prefix), timeout: timeout, budget: scanBudget, stop: func() {}}
}

// Close closes the connection.
func (db *DB) Close() error {
	err := db.raw.Close()
	db.stop()
	return err
}

// DeleteAll deletes every entry of the namespace.
func (db *DB) DeleteAll(ctx context.Context) error {
	return db.raw.DeleteRange(ctx, db.keys.prefix, prefixEnd(db.keys.prefix))
}

// Ping checks that TiKV can be read.
func (db *DB) Ping(ctx context.Context) error {
	_, err := db.get(ctx, db.keys.meta("format"))
	return err
}

func (db *DB) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if db.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, db.timeout)
}

func (db *DB) get(ctx context.Context, key []byte) ([]byte, error) {
	ctx, cancel := db.withTimeout(ctx)
	defer cancel()
	return db.raw.Get(ctx, key)
}

func (db *DB) batchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	ctx, cancel := db.withTimeout(ctx)
	defer cancel()
	return db.raw.BatchGet(ctx, keys)
}

func (db *DB) put(ctx context.Context, key, value []byte, ttl uint64) error {
	ctx, cancel := db.withTimeout(ctx)
	defer cancel()
	return db.raw.PutWithTTL(ctx, key, value, ttl)
}

// putAll writes entries with the same TTL.
func (db *DB) putAll(ctx context.Context, keys, values [][]byte, ttl uint64) error {
	if len(keys) == 1 {
		return db.put(ctx, keys[0], values[0], ttl)
	}
	ctx, cancel := db.withTimeout(ctx)
	defer cancel()
	ttls := make([]uint64, len(keys))
	for i := range ttls {
		ttls[i] = ttl
	}
	return db.raw.BatchPutWithTTL(ctx, keys, values, ttls)
}

func (db *DB) compareAndSwap(ctx context.Context, key, previous, value []byte) ([]byte, bool, error) {
	ctx, cancel := db.withTimeout(ctx)
	defer cancel()
	return db.raw.CompareAndSwap(ctx, key, previous, value)
}

// scanLimit returns how many entries a scan may ask for when keys are at
// most maxKey bytes and values at most maxValue bytes.
func (db *DB) scanLimit(maxKey, maxValue int) int {
	return max(1, min(maxScanLimit, db.budget/(maxKey+maxValue+pairOverhead)))
}

// scan calls fn for every entry in [start, end), in key order, reading
// limit entries at a time. fn must not keep the slices it is given.
func (db *DB) scan(ctx context.Context, start, end []byte, limit int, fn func(key, value []byte) error) error {
	for {
		sctx, cancel := db.withTimeout(ctx)
		keys, values, err := db.raw.Scan(sctx, start, end, limit)
		cancel()
		if err != nil {
			return err
		}
		for i, k := range keys {
			if err := fn(k, values[i]); err != nil {
				return err
			}
		}
		// A scan returns fewer entries than asked for only at the end of
		// the range.
		if len(keys) < limit {
			return nil
		}
		start = append(append([]byte(nil), keys[len(keys)-1]...), 0)
	}
}

// ttlSeconds is a TTL for TiKV, or false if keepUntil has passed. retention
// 0 keeps entries forever (TTL 0).
func ttlSeconds(retention time.Duration, keepUntil, now time.Time) (uint64, bool) {
	if retention <= 0 {
		return 0, true
	}
	left := keepUntil.Sub(now)
	if left < time.Second {
		return 0, false
	}
	return uint64(left / time.Second), true
}

// CheckLayout records the layout of the TiKV data on first use and refuses
// to run with a different one: changing the chunk range would make reads
// miss chunks that start before the queried range. storage.index_buckets
// does not apply: TiKV splits large ranges into regions by itself.
func (db *DB) CheckLayout(ctx context.Context, chunkRange time.Duration) error {
	fields := []struct{ name, value, option string }{
		{"format", fmt.Sprint(formatVersion), "the storage format"},
		{"chunk_range_ms", fmt.Sprint(chunkRange.Milliseconds()), "storage.chunk_range"},
	}
	for _, f := range fields {
		prev, swapped, err := db.compareAndSwap(ctx, db.keys.meta(f.name), nil, []byte(f.value))
		if err != nil {
			return fmt.Errorf("tikv meta: %w", err)
		}
		if !swapped && string(prev) != f.value {
			return fmt.Errorf("%s is %s but the data in TiKV was written with %s (%s); "+
				"changing it is not supported", f.option, f.value, prev, f.name)
		}
	}
	return nil
}
