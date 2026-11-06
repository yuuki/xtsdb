package storagetest

import (
	"testing"
	"time"

	"github.com/yuuki/xtsdb/internal/storage"
)

// The in-memory stores are the reference of the conformance tests.
func TestColdConformance(t *testing.T) {
	RunColdStoreTests(t, func(_ *testing.T, retention time.Duration, chunkRange int64) storage.ColdStore {
		c := NewCold(chunkRange)
		c.Retention = retention
		return c
	})
}

func TestIndexConformance(t *testing.T) {
	RunIndexTests(t, func(_ *testing.T, retention time.Duration) IndexStore {
		x := NewIndex()
		x.Retention = retention
		return x
	})
}
