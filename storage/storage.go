package storage

import (
	"log"

	vmstorage "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// AddRows adds mrs to the storage.
func AddRows(mrs []vmstorage.MetricRow) error {
	log.Printf("%+v\n", mrs)
	return nil
}
