package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/labstack/gommon/log"
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
		listenAddr string
	)

	flags := flag.NewFlagSet("xtsdb-ingester", flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() {
		fmt.Fprint(cli.errStream, helpText)
	}
	flags.StringVar(&listenAddr, "graphiteListenAddr", "", "")

	if listenAddr == "" {
		log.Printf("any of ListenAddr option is required")
	}

	return 0
}

var helpText = `
Usage: xtsdb-ingester [options]

Options:
  --graphiteListenAddr=ADDR       Listen Address for Graphite protocol (required)
`
