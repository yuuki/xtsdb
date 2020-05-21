package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/yuuki/xtsdb/config"
	"github.com/yuuki/xtsdb/ingester"
	"github.com/yuuki/xtsdb/storage"
)

const (
	exitCodeOK  = 0
	exitCodeErr = 10 + iota
)

// CLI is the command line object.
type CLI struct {
	// outStream and errStream are the stdout and stderr
	// to write message from the CLI.
	outStream, errStream io.Writer
}

func main() {
	cli := &CLI{outStream: os.Stdout, errStream: os.Stderr}
	os.Exit(cli.Run(os.Args))
}

// Run invokes the CLI with the given arguments.
func (cli *CLI) Run(args []string) int {
	log.SetOutput(cli.errStream)

	var (
		listenAddr      string
		durationExpires string

		profile bool
	)

	flags := flag.NewFlagSet("xtsdb-ingester", flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() {
		fmt.Fprint(cli.errStream, helpText)
	}
	flags.StringVar(&listenAddr, "graphiteListenAddr", "", "")
	flags.StringVar(&durationExpires, "durationExpires", "1h", "")
	flags.BoolVar(&profile, "profile", false, "")
	if err := flags.Parse(args[1:]); err != nil {
		return exitCodeErr
	}

	ts, err := time.ParseDuration(durationExpires)
	if err != nil {
		log.Printf("Could not parse %s : %s\n", durationExpires, err)
	}
	config.Config.DurationExpires = ts

	storage.Init()

	if listenAddr == "" {
		log.Println("any of ListenAddr option is required")
		return exitCodeErr
	}

	if profile {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	log.Println("Starting xtsdb-ingester...")
	if err := ingester.Serve(listenAddr); err != nil {
		log.Printf("%+v\n", err)
		return exitCodeErr
	}

	return exitCodeOK
}

var helpText = `
Usage: xtsdb-ingester [options]

Options:
  --graphiteListenAddr=ADDR       Listen Address for Graphite protocol (required)
`
