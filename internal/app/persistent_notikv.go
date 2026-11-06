//go:build notikv || (darwin && !cgo)

package app

import (
	"context"
	"errors"
	"log/slog"

	"github.com/yuuki/xtsdb/internal/config"
)

// TiKVSupported reports whether this build includes the TiKV backend.
const TiKVSupported = false

var errNoTiKV = errors.New("this build of xtsdb does not include the tikv backend " +
	"(built with the notikv tag, or for macOS without cgo)")

func openTiKV(context.Context, *config.Config, *slog.Logger) (*Persistent, error) {
	return nil, errNoTiKV
}

func migrateTiKV(context.Context, *config.Config, *slog.Logger) error { return errNoTiKV }
