package querier

import (
	"github.com/VictoriaMetrics/metricsql"
	"golang.org/x/xerrors"
)

func execQuery(query string) error {
	expr, err := metricsql.Parse(query)
	if err != nil {
		return xerrors.Errorf(
			"error when executing query=%q for (time=%d, step=%d): %w",
			query, start, step, err)
	}

}
