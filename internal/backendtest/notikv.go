//go:build notikv || (darwin && !cgo)

package backendtest

import (
	"testing"

	"github.com/yuuki/xtsdb/internal/config"
)

const tikvSupported = false

func deleteTiKV(*testing.T, config.TiKVConfig, string) {}
