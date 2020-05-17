package redis

import (
	"fmt"
	"log"
	"time"

	vmstorage "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"

	goredis "github.com/go-redis/redis/v7"
	"golang.org/x/xerrors"
)

const (
	// prefixEx is a prefix of expired keys.
	prefixKeyForExpire = "ex:"
	durationForExpire  = 5 * time.Second
)

// Redis provides a redis client.
type Redis struct {
	client *goredis.Client
}

// New creates a Redis client.
func New() (*Redis, error) {
	r := goredis.NewClient(&goredis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})
	if err := r.Ping().Err(); err != nil {
		return nil, xerrors.Errorf("could not ping: %w", err)
	}
	return &Redis{client: r}, nil
}

// AddRows inserts rows into redis-server.
func (r *Redis) AddRows(mrs []vmstorage.MetricRow) error {
	for _, row := range mrs {
		mname := string(row.MetricNameRaw)

		// TODO: redis lua scripting
		err := r.client.XAdd(&goredis.XAddArgs{
			Stream: mname,
			ID:     fmt.Sprintf("%d", row.Timestamp),
			Values: map[string]interface{}{"": row.Value},
		}).Err()
		if err != nil {
			log.Printf("Could not add stream: %s", err)
			continue
		}

		// TODO: check expired and set
		key := prefixKeyForExpire + mname
		if err := r.client.Set(key, true, durationForExpire).Err(); err != nil {
			log.Printf("Could not set stream: %s", err)
			continue
		}
		fmt.Printf("Set expire to key '%s'\n", key)
	}

	return nil
}
