//go:build !notikv && (!darwin || cgo)

package backendtest

import (
	"context"
	"testing"

	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/tikv"
)

// tikvSupported reports whether this build includes the TiKV backend.
const tikvSupported = true

// deleteTiKV deletes the data of a test namespace in TiKV.
func deleteTiKV(t *testing.T, cfg config.TiKVConfig, prefix string) {
	db, err := tikv.Open(context.Background(), cfg, prefix, nil)
	if err != nil {
		t.Logf("cleanup: %v", err)
		return
	}
	defer db.Close()
	if err := db.DeleteAll(context.Background()); err != nil {
		t.Logf("cleanup: %v", err)
	}
}
