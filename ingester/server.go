package ingester

import (
	"io"
	"log"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/ingestserver/graphite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	vmparser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/graphite"

	"github.com/yuuki/xtsdb/storage"
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
	return ctx.FlushBufs()
}

// Serve runs a server.
func Serve(addr string) error {
	// Start
	go func() {
		log.Println("Starting streamer of old data points")
		if err := storage.StreamVolatileDataPoints(); err != nil {
			log.Printf("%+v\n")
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