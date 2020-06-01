package flusher

import (
	"log"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/yuuki/xtsdb/storage"
)

// Serve runs a server of flusher.
func Serve() error {
	go func() {
		// Expose the registered metrics at `/metrics` path.
		http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
			metrics.WritePrometheus(w, true)
		})
		log.Fatal(http.ListenAndServe(":8081", nil))
	}()

	if err := storage.FlushVolatileDataPoints(); err != nil {
		return err
	}

	// TODO: Wait until goroutines stop
	sig := procutil.WaitForSigterm()
	log.Printf("received signal %s\n", sig)

	return nil
}
