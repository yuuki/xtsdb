// Command xtsdb is a time series database that tiers data between Redis
// and a persistent store, Cassandra or TiKV.
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
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/version"
)

const usage = `Usage: xtsdb <command> [flags]

Commands:
  all        Run the ingester, flusher and querier in one process
  ingester   Accept writes (Prometheus remote write, Graphite)
  flusher    Move closed chunks from Redis to the persistent tier
  querier    Serve the Prometheus-compatible query API
  migrate    Create or update the Cassandra schema (or check TiKV) and exit
  import     Import historical data (Prometheus TSDB blocks, OpenMetrics)
  quarantine Inspect or delete undecodable chunks kept in Redis
  config     Print the effective configuration and exit
  version    Print version information and exit

Run 'xtsdb <command> -h' for the flags of a command.
`

const importUsage = `Usage: xtsdb import [flags] <path>...

Imports historical samples directly into the persistent tier (Cassandra or
TiKV), bypassing Redis.
With -format=tsdb, each path is a Prometheus data directory or a block
directory; with -format=openmetrics, each path is an OpenMetrics text file
in which every sample has a timestamp ("-" reads standard input). Importing
the same data again is safe; -state-file skips the blocks and files that
were imported completely.

Flags:
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
	switch args[0] {
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stdout, usage)
		return 0
	case "version", "-version", "--version":
		info := version.Info()
		fmt.Fprintf(stdout, "xtsdb %s (revision %s, date %s, %s)\n",
			info["version"], info["revision"], info["buildDate"], info["goVersion"])
		return 0
	}

	c, code := parseCommand(args, stderr)
	if c == nil {
		return code
	}
	cfg, err := c.loadConfig()
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
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

	return c.execute(ctx, cfg, logger, stdout, stderr)
}

// command is a parsed command line: a command with its flags and arguments.
type command struct {
	cmd  string
	sub  string // the subcommand of quarantine
	name string // the command and its subcommand, as in "quarantine list"

	configFile string
	expandEnv  bool
	logLevel   string
	all        bool          // the -all flag of quarantine delete
	imp        importOptions // the flags of import

	args []string // the arguments after the flags
}

// parseCommand parses a command line, args, that starts with the command.
// If the command is not to be run, because the command line is invalid or
// help was asked for, it returns nil and the exit code, and the error or
// the help has been written to stderr.
func parseCommand(args []string, stderr io.Writer) (*command, int) {
	c := &command{cmd: args[0], name: args[0]}
	rest := args[1:]
	if c.cmd == "quarantine" {
		if len(rest) == 0 || (rest[0] != "list" && rest[0] != "delete") {
			fmt.Fprint(stderr, quarantineUsage)
			return nil, 2
		}
		c.sub, rest = rest[0], rest[1:]
		c.name += " " + c.sub
	}
	fs := c.flagSet(stderr)
	if err := fs.Parse(rest); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, 0
		}
		return nil, 2
	}
	if !c.checkArgs(fs, stderr) {
		return nil, 2
	}
	c.args = fs.Args()
	return c, 0
}

// flagSet returns the flags of the command, which it parses into c.
func (c *command) flagSet(stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("xtsdb "+c.name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.configFile, "config.file", "", "Path to the YAML configuration file.")
	fs.BoolVar(&c.expandEnv, "config.expand-env", false, "Expand ${VAR} references in the configuration file.")
	fs.StringVar(&c.logLevel, "log.level", "", "Override log.level (debug, info, warn, error).")
	if c.sub == "delete" {
		fs.BoolVar(&c.all, "all", false, "Delete every quarantined chunk.")
	}
	if c.cmd == "import" {
		fs.StringVar(&c.imp.format, "format", "tsdb", "Input format: tsdb (Prometheus TSDB blocks) or openmetrics.")
		fs.StringVar(&c.imp.stateFile, "state-file", "", "File recording the blocks and files imported completely; they are skipped when the import is run again.")
		fs.IntVar(&c.imp.concurrency, "concurrency", 16, "Series imported at a time.")
		fs.IntVar(&c.imp.maxBuffered, "max-buffered-samples", 10_000_000,
			"OpenMetrics: samples held in memory before they are imported (about 32 bytes each); input files are mapped into memory, standard input is read completely.")
		fs.Usage = func() {
			fmt.Fprint(stderr, importUsage)
			fs.PrintDefaults()
		}
	}
	return fs
}

// checkArgs reports whether the arguments left after the flags of fs suit
// the command. If they do not, it writes why to stderr.
func (c *command) checkArgs(fs *flag.FlagSet, stderr io.Writer) bool {
	if c.cmd == "import" && fs.NArg() == 0 {
		fs.Usage()
		return false
	}
	if fs.NArg() > 0 && c.sub != "delete" && c.cmd != "import" {
		fmt.Fprintf(stderr, "error: unexpected argument %q (flags must come after the command, e.g. xtsdb %s -config.file=...)\n", fs.Arg(0), c.name)
		return false
	}
	if c.sub == "delete" && (fs.NArg() > 0) == c.all {
		fmt.Fprint(stderr, "error: give either quarantine keys or -all\n\n"+quarantineUsage)
		return false
	}
	return true
}

// loadConfig loads the configuration file, with the log level of -log.level
// if it is set.
func (c *command) loadConfig() (*config.Config, error) {
	cfg, err := config.Load(c.configFile, c.expandEnv)
	if err != nil {
		return nil, err
	}
	if c.logLevel != "" {
		cfg.Log.Level = c.logLevel
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// execute runs the command and returns its exit code.
func (c *command) execute(ctx context.Context, cfg *config.Config, logger *slog.Logger, stdout, stderr io.Writer) int {
	switch c.cmd {
	case "config":
		if err := printConfig(cfg, stdout); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		return 0
	case "migrate":
		if err := app.Migrate(ctx, cfg, logger); err != nil {
			logger.Error("migration failed", "err", err)
			return 1
		}
		return 0
	case "import":
		if err := runImport(ctx, cfg, logger, c.imp, c.args, stdout); err != nil {
			logger.Error("import failed", "err", err)
			return 1
		}
		return 0
	case "quarantine":
		if err := quarantine(ctx, cfg, logger, c.sub, c.args, stdout); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		return 0
	}

	target, err := app.ParseTarget(c.cmd)
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

// printConfig prints the configuration, with its secrets redacted.
func printConfig(cfg *config.Config, stdout io.Writer) error {
	redacted := *cfg
	redacted.Redis.Password = redact(cfg.Redis.Password)
	redacted.Cassandra.Password = redact(cfg.Cassandra.Password)
	redacted.Server.Auth.Read = cfg.Server.Auth.Read.Redacted()
	redacted.Server.Auth.Write = cfg.Server.Auth.Write.Redacted()
	out, err := yaml.Marshal(&redacted)
	if err != nil {
		return err
	}
	_, _ = stdout.Write(out)
	return nil
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
