package ingester

import (
	"io"
	"log"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/ingestserver/graphite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	vmparser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/graphite"
	"github.com/VictoriaMetrics/metrics"

	"github.com/yuuki/xtsdb/storage"
)

var (
	rowsInserted  = metrics.NewCounter(`xt_rows_inserted_total`)
	rowsPerInsert = metrics.NewHistogram(`xt_rows_per_insert`)
)

// insertHandler processes remote write for graphite plaintext protocol.
//
// Copy code from github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/graphite.
func insertHandler(r io.Reader) error {
	return vmparser.ParseStream(r, insertRows)
}

// Copy code from github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/graphite.
func insertRows(rows []vmparser.Row) error {
	ctx := GetInsertCtx()
	defer PutInsertCtx(ctx)

	ctx.Reset(len(rows))
	for i := range rows {
		r := &rows[i]
		ctx.Labels = ctx.Labels[:0]
		ctx.AddLabel("", r.Metric)
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.AddLabel(tag.Key, tag.Value)
		}
		ctx.WriteDataPoint(nil, ctx.Labels, r.Timestamp, r.Value)
	}
	rowsInserted.Add(len(rows))
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}

// Serve runs a server.
func Serve(addr string) error {
	go func() {
		// Expose the registered metrics at `/metrics` path.
		http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
			metrics.WritePrometheus(w, true)
		})
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// Start
	go func() {
		log.Println("Starting streamer of old data points")
		if err := storage.StreamVolatileDataPoints(); err != nil {
			log.Printf("%+v\n", err)
		}
		log.Println("Shutdown streamer of old data points")
	}()

	graphiteServer := graphite.MustStart(addr, insertHandler)
	defer graphiteServer.MustStop()

	// TODO: Wait until goroutines stop
	sig := procutil.WaitForSigterm()
	log.Printf("received signal %s\n", sig)

	return nil
}
