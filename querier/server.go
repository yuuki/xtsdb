package querier

import (
	"fmt"
	"log"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/phyber/negroni-gzip/gzip"
	"github.com/rs/cors"
	"github.com/urfave/negroni"
)

const defaultStep = 5 * 60 * 1000

// queryHandler processes /api/v1/query request.
//
// See https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
func queryHandler(w http.ResponseWriter, r *http.Request) {
	ct := currentTime()

	query := r.FormValue("query")
	if len(query) == 0 {
		return fmt.Errorf("missing `query` arg")
	}
	// start := r.FormValue("time")

	// step, err := r.FormValue(r, "step")
	// if err != nil {
	// 	return err
	// }
	// if step <= 0 {
	// 	step = defaultStep
	// }

	if err := execQuery(query); err != nil {
		log.Fataln(err)
	}

	w.Header().Set("Content-Type", "application/json")
}

// Serve runs a server.
func Serve(addr string) error {
	go func() {
		n := negroni.New()
		n.Use(negroni.NewRecovery())
		n.Use(negroni.NewLogger())
		n.Use(gzip.Gzip(gzip.DefaultCompression))
		n.Use(cors.New(cors.Options{
			AllowedOrigins: []string{"*"},
			AllowedMethods: []string{"GET", "POST"},
			AllowedHeaders: []string{"Origin", "Accept", "Content-Type"},
		}))

		n.UseHandler(mux)
		mux := http.NewServeMux()
		mux.Handle("/api/v0/query", http.HandlerFunc(queryHandler))

		srv := &http.Server{Addr: addr, Handler: n}

		if err := srv.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()

	sig := procutil.WaitForSigterm()
	log.Printf("received signal %s\n", sig)

	return nil
}
