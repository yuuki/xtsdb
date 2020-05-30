package querier

import (
	"github.com/VictoriaMetrics/metricsql"
	"golang.org/x/xerrors"
)

func execQuery(query string) error {
	_, err := metricsql.Parse(query)
	if err != nil {
		return xerrors.Errorf("error when executing query=%q: %w", query, err)
	}
	return nil
}
