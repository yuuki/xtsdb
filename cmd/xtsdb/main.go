// Command xtsdb is a time series database that tiers data between Redis
// and Cassandra.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	yaml "go.yaml.in/yaml/v4"

	"github.com/yuuki/xtsdb/internal/app"
	"github.com/yuuki/xtsdb/internal/cassandra"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/version"
)

const usage = `Usage: xtsdb <command> [flags]

Commands:
  all        Run the ingester, flusher and querier in one process
  ingester   Accept writes (Prometheus remote write, Graphite)
  flusher    Move closed chunks from Redis to Cassandra
  querier    Serve the Prometheus-compatible query API
  migrate    Create or update the Cassandra schema and exit
  config     Print the effective configuration and exit
  version    Print version information and exit

Run 'xtsdb <command> -h' for the flags of a command.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd := args[0]
	switch cmd {
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stdout, usage)
		return 0
	case "version", "-version", "--version":
		info := version.Info()
		fmt.Fprintf(stdout, "xtsdb %s (revision %s, date %s, %s)\n",
			info["version"], info["revision"], info["buildDate"], info["goVersion"])
		return 0
	}

	fs := flag.NewFlagSet("xtsdb "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	configFile := fs.String("config.file", "", "Path to the YAML configuration file.")
	expandEnv := fs.Bool("config.expand-env", false, "Expand ${VAR} references in the configuration file.")
	logLevel := fs.String("log.level", "", "Override log.level (debug, info, warn, error).")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "error: unexpected argument %q (flags must come after the command, e.g. xtsdb %s -config.file=...)\n", fs.Arg(0), cmd)
		return 2
	}

	cfg, err := config.Load(*configFile, *expandEnv)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	if *logLevel != "" {
		cfg.Log.Level = *logLevel
		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
	}
	logger := newLogger(cfg.Log, stderr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal starts a graceful shutdown, restore the default
	// handling so that a second signal terminates immediately.
	go func() {
		<-ctx.Done()
		stop()
	}()

	switch cmd {
	case "config":
		redacted := *cfg
		redacted.Redis.Password = redact(cfg.Redis.Password)
		redacted.Cassandra.Password = redact(cfg.Cassandra.Password)
		out, err := yaml.Marshal(&redacted)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		_, _ = stdout.Write(out)
		return 0
	case "migrate":
		if err := cassandra.Migrate(ctx, cfg.Cassandra, logger); err != nil {
			logger.Error("migration failed", "err", err)
			return 1
		}
		return 0
	}

	target, err := app.ParseTarget(cmd)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n\n%s", err, usage)
		return 2
	}
	a, err := app.New(ctx, target, cfg, logger)
	if err != nil {
		logger.Error("startup failed", "err", err)
		return 1
	}
	if err := a.Run(ctx); err != nil {
		logger.Error("xtsdb failed", "err", err)
		return 1
	}
	return 0
}

func redact(secret string) string {
	if secret == "" {
		return ""
	}
	return "<redacted>"
}

func newLogger(cfg config.LogConfig, w io.Writer) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(strings.ToUpper(cfg.Level)))
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(cfg.Format, "json") {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
