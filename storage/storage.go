package storage

import (
	"log"

	"github.com/yuuki/xtsdb/config"
	"github.com/yuuki/xtsdb/storage/cassandra"
	"github.com/yuuki/xtsdb/storage/model"
	"github.com/yuuki/xtsdb/storage/redis"
)

type Storage struct {
	Memstore *redis.Redis
	// diskstore
}

var Store *Storage

// Init creates a storage client.
func Init() {
	r, err := redis.New(config.Config.RedisAddrs)
	if err != nil {
		log.Fatal(err)
	}
	Store = &Storage{Memstore: r}
	// mrsChan = make(chan model.MetricRows, 1500)
	// RunMemWriter(1500)
}

var mrsChan chan model.MetricRows

func RunMemWriter(num int) {
	for i := 0; i < num; i++ {
		go func(mrsc <-chan model.MetricRows) {
			for mrs := range mrsc {
				// dispatch job
				if err := AddRows(mrs); err != nil {
					log.Printf("%+v", err)
				}
			}
		}(mrsChan)
	}
}

func SubmitMemWriter(mrs model.MetricRows) error {
	mrsChan <- mrs
	return nil
}

// AddRows adds mrs to the storage.
func AddRows(mrs model.MetricRows) error {
	if err := Store.Memstore.AddRows(mrs); err != nil {
		return err
	}
	return nil
}

// StreamVolatileDataPoints streams volatile datapoints to reliable stream.
func StreamVolatileDataPoints() error {
	addr := config.Config.RedisPubSubAddr
	r, err := redis.New([]string{addr})
	if err != nil {
		return err
	}
	log.Printf("Subscribing expired events from '%s'", addr)

	if err := r.SubscribeExpiredDataPoints(addr); err != nil {
		return err
	}

	return nil
}

// FlushVolatileDataPoints runs a loop of flushing data points
// from MemStore to DiskStore.
func FlushVolatileDataPoints() error {
	r, err := redis.New(config.Config.RedisAddrs)
	if err != nil {
		return err
	}
	c, err := cassandra.New()
	if err != nil {
		return err
	}
	err = r.FlushExpiredDataPoints(
		func(mid string, datapoints []byte) error {
			return c.AddRows(mid, datapoints)
		},
	)
	if err != nil {
		return err
	}
	return nil

}
