package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/yuuki/xtsdb/flusher"
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
		workers int
	)

	flags := flag.NewFlagSet("xtsdb-ingester", flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.IntVar(&workers, "workers", runtime.GOMAXPROCS(-1), "")
	flags.Usage = func() {
		fmt.Fprint(cli.errStream, helpText)
	}
	if err := flags.Parse(args[1:]); err != nil {
		return exitCodeErr
	}

	storage.Init()

	log.Println("Starting xtsdb-flusher...")
	if err := flusher.Serve(workers); err != nil {
		log.Printf("%+v\n", err)
		return exitCodeErr
	}

	return exitCodeOK
}

var helpText = `
Usage: xtsdb-flusher [options]

Options:
`
