package flusher

import (
	"log"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/yuuki/xtsdb/storage"
)

// Serve runs a server of flusher.
func Serve() error {
	if err := storage.FlushVolatileDataPoints(); err != nil {
		return err
	}

	// TODO: Wait until goroutines stop
	sig := procutil.WaitForSigterm()
	log.Printf("received signal %s\n", sig)

	return nil
}
