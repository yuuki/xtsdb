//go:build !notikv && (!darwin || cgo)

package tikv

import (
	"context"
	"log/slog"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logOnce sync.Once

// redirectLogs routes the logs of the TiKV and PD clients, which use a
// global zap logger, to slog. The clients log routine events (region cache
// updates, PD leader changes) at info level, so info becomes debug.
func redirectLogs(logger *slog.Logger) {
	if logger == nil {
		return
	}
	logOnce.Do(func() {
		core := &slogCore{logger: logger.With("component", "tikv-client")}
		props := &log.ZapProperties{Core: core, Syncer: zapcore.AddSync(nopSyncer{}), Level: zap.NewAtomicLevelAt(zapcore.InfoLevel)}
		log.ReplaceGlobals(zap.New(core), props)
	})
}

type nopSyncer struct{}

func (nopSyncer) Write(p []byte) (int, error) { return len(p), nil }

// slogCore is a zapcore.Core that writes to slog.
type slogCore struct {
	logger *slog.Logger
	attrs  []any
}

func slogLevel(l zapcore.Level) slog.Level {
	switch {
	case l >= zapcore.ErrorLevel:
		return slog.LevelError
	case l == zapcore.WarnLevel:
		return slog.LevelWarn
	case l == zapcore.InfoLevel:
		return slog.LevelDebug
	default:
		return slog.LevelDebug - 4
	}
}

func (c *slogCore) Enabled(l zapcore.Level) bool {
	return c.logger.Enabled(context.Background(), slogLevel(l))
}

func (c *slogCore) With(fields []zapcore.Field) zapcore.Core {
	return &slogCore{logger: c.logger, attrs: append(append([]any(nil), c.attrs...), fieldAttrs(fields)...)}
}

func (c *slogCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c *slogCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	attrs := append(append([]any(nil), c.attrs...), fieldAttrs(fields)...)
	if e.LoggerName != "" {
		attrs = append(attrs, "logger", e.LoggerName)
	}
	c.logger.Log(context.Background(), slogLevel(e.Level), e.Message, attrs...)
	return nil
}

func (c *slogCore) Sync() error { return nil }

func fieldAttrs(fields []zapcore.Field) []any {
	if len(fields) == 0 {
		return nil
	}
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	attrs := make([]any, 0, 2*len(enc.Fields))
	for k, v := range enc.Fields {
		attrs = append(attrs, k, v)
	}
	return attrs
}
