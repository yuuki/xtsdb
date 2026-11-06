//go:build !notikv && (!darwin || cgo)

package tikv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/rawkv"
)

// memKV is an in-memory rawClient with the semantics of TiKV RawKV that the
// stores rely on, and its limits: keys of at most 8 KiB (max-key-size) and
// responses of at most 10 MiB (server.max-grpc-send-msg-len).
type memKV struct {
	mu      sync.Mutex
	entries map[string]memEntry
	now     func() time.Time
	// Scans and BatchGets count read requests.
	Scans, BatchGets int
}

type memEntry struct {
	value   []byte
	expires time.Time // zero: never
}

const (
	memMaxKey      = 8 << 10
	memMaxResponse = 10 << 20
)

var _ rawClient = (*memKV)(nil)

func newMemKV() *memKV {
	return &memKV{entries: make(map[string]memEntry), now: time.Now}
}

func (m *memKV) live(key string) ([]byte, bool) {
	e, ok := m.entries[key]
	if !ok || (!e.expires.IsZero() && !m.now().Before(e.expires)) {
		return nil, false
	}
	return e.value, true
}

func (m *memKV) set(key, value []byte, ttl uint64) error {
	if len(key) > memMaxKey {
		return fmt.Errorf("key is too large: %d bytes", len(key))
	}
	if len(value) == 0 {
		return errors.New("empty value")
	}
	e := memEntry{value: slices.Clone(value)}
	if ttl > 0 {
		e.expires = m.now().Add(time.Duration(ttl) * time.Second)
	}
	m.entries[string(key)] = e
	return nil
}

// TTL returns the remaining TTL of a key, 0 if it has none, and false if it
// does not exist.
func (m *memKV) TTL(key []byte) (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.live(string(key)); !ok {
		return 0, false
	}
	e := m.entries[string(key)]
	if e.expires.IsZero() {
		return 0, true
	}
	return e.expires.Sub(m.now()), true
}

// Keys returns the live keys with a prefix, sorted.
func (m *memKV) Keys(prefix []byte) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out [][]byte
	for k := range m.entries {
		if _, ok := m.live(k); ok && strings.HasPrefix(k, string(prefix)) {
			out = append(out, []byte(k))
		}
	}
	slices.SortFunc(out, bytes.Compare)
	return out
}

func (m *memKV) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, string(key))
}

func (m *memKV) Get(ctx context.Context, key []byte, _ ...rawkv.RawOption) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, _ := m.live(string(key))
	return slices.Clone(v), nil
}

func (m *memKV) BatchGet(ctx context.Context, keys [][]byte, _ ...rawkv.RawOption) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.BatchGets++
	out := make([][]byte, len(keys))
	size := 0
	for i, k := range keys {
		if v, ok := m.live(string(k)); ok {
			out[i] = slices.Clone(v)
			size += len(k) + len(v)
		}
	}
	if size > memMaxResponse {
		return nil, fmt.Errorf("response of %d bytes exceeds the gRPC limit", size)
	}
	return out, nil
}

func (m *memKV) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, _ ...rawkv.RawOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.set(key, value, ttl)
}

func (m *memKV) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, _ ...rawkv.RawOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) != len(values) || len(keys) != len(ttls) {
		return errors.New("mismatched batch")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range keys {
		if err := m.set(keys[i], values[i], ttls[i]); err != nil {
			return err
		}
	}
	return nil
}

func (m *memKV) Scan(ctx context.Context, startKey, endKey []byte, limit int, _ ...rawkv.RawOption) ([][]byte, [][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if limit > rawkv.MaxRawKVScanLimit {
		return nil, nil, rawkv.ErrMaxScanLimitExceeded
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Scans++
	var keys []string
	for k := range m.entries {
		if _, ok := m.live(k); ok && k >= string(startKey) && (len(endKey) == 0 || k < string(endKey)) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	keys = keys[:min(limit, len(keys))]
	var (
		outK, outV [][]byte
		size       int
	)
	for _, k := range keys {
		v, _ := m.live(k)
		outK, outV = append(outK, []byte(k)), append(outV, slices.Clone(v))
		size += len(k) + len(v)
	}
	if size > memMaxResponse {
		return nil, nil, fmt.Errorf("response of %d bytes exceeds the gRPC limit", size)
	}
	return outK, outV, nil
}

func (m *memKV) CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, _ ...rawkv.RawOption) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.live(string(key))
	if (previousValue == nil && !ok) || (previousValue != nil && ok && bytes.Equal(cur, previousValue)) {
		return slices.Clone(cur), true, m.set(key, newValue, 0)
	}
	return slices.Clone(cur), false, nil
}

func (m *memKV) DeleteRange(ctx context.Context, startKey, endKey []byte, _ ...rawkv.RawOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.entries {
		if k >= string(startKey) && (len(endKey) == 0 || k < string(endKey)) {
			delete(m.entries, k)
		}
	}
	return nil
}

func (m *memKV) Close() error { return nil }
