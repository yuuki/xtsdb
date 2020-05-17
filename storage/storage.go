package storage

import (
	"log"

	vmstorage "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"

	"github.com/yuuki/xtsdb/storage/redis"
)

type Storage struct {
	Memstore *redis.Redis
	// diskstore
}

var Store *Storage

// Init creates a storage client.
func Init() {
	r, err := redis.New()
	if err != nil {
		log.Fatal(err)
	}
	Store = &Storage{Memstore: r}
}

// AddRows adds mrs to the storage.
func AddRows(mrs []vmstorage.MetricRow) error {
	if err := Store.Memstore.AddRows(mrs); err != nil {
		return err
	}
	return nil
}
