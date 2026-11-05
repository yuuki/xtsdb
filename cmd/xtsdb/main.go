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
	"text/tabwriter"
	"time"

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
  quarantine Inspect or delete undecodable chunks kept in Redis
  config     Print the effective configuration and exit
  version    Print version information and exit

Run 'xtsdb <command> -h' for the flags of a command.
`

const quarantineUsage = `Usage:
  xtsdb quarantine list   [flags]           List quarantined chunks
  xtsdb quarantine delete [flags] <key>...  Delete quarantined chunks
  xtsdb quarantine delete [flags] -all      Delete every quarantined chunk
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

	name, rest := cmd, args[1:]
	var sub string
	if cmd == "quarantine" {
		if len(rest) == 0 || (rest[0] != "list" && rest[0] != "delete") {
			fmt.Fprint(stderr, quarantineUsage)
			return 2
		}
		sub, rest = rest[0], rest[1:]
		name += " " + sub
	}
	fs := flag.NewFlagSet("xtsdb "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	configFile := fs.String("config.file", "", "Path to the YAML configuration file.")
	expandEnv := fs.Bool("config.expand-env", false, "Expand ${VAR} references in the configuration file.")
	logLevel := fs.String("log.level", "", "Override log.level (debug, info, warn, error).")
	var all *bool
	if sub == "delete" {
		all = fs.Bool("all", false, "Delete every quarantined chunk.")
	}
	if err := fs.Parse(rest); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 && sub != "delete" {
		fmt.Fprintf(stderr, "error: unexpected argument %q (flags must come after the command, e.g. xtsdb %s -config.file=...)\n", fs.Arg(0), name)
		return 2
	}
	if sub == "delete" && (fs.NArg() > 0) == *all {
		fmt.Fprint(stderr, "error: give either quarantine keys or -all\n\n"+quarantineUsage)
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
		redacted.Server.Auth.Read = cfg.Server.Auth.Read.Redacted()
		redacted.Server.Auth.Write = cfg.Server.Auth.Write.Redacted()
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
	case "quarantine":
		if err := quarantine(ctx, cfg, logger, sub, fs.Args(), stdout); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
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

// quarantine lists or deletes undecodable chunks that the flusher moved
// out of the way.
func quarantine(ctx context.Context, cfg *config.Config, logger *slog.Logger, sub string, keys []string, stdout io.Writer) error {
	store, client, err := app.OpenHot(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	if sub == "delete" && len(keys) > 0 {
		n, err := store.DeleteQuarantined(ctx, keys)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "deleted %d quarantined chunks\n", n)
		return nil
	}
	chunks, err := store.ListQuarantined(ctx)
	if err != nil {
		return err
	}
	if sub == "delete" {
		keys := make([]string, len(chunks))
		for i, c := range chunks {
			keys[i] = c.Key
		}
		n, err := store.DeleteQuarantined(ctx, keys)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "deleted %d quarantined chunks\n", n)
		return nil
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tSERIES\tWINDOW START\tBYTES\tHEADER\tEXPIRES")
	for _, c := range chunks {
		expires := "never"
		if !c.Expires.IsZero() {
			expires = c.Expires.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%q\t%s\n", c.Key, c.Series,
			time.UnixMilli(c.Start).UTC().Format(time.RFC3339Nano), c.Bytes, c.Header, expires)
	}
	return tw.Flush()
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
