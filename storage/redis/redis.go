package redis

import (
	"fmt"
	"log"
	"strings"
	"time"

	vmstorage "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"

	goredis "github.com/go-redis/redis/v7"
	"golang.org/x/xerrors"
)

const (
	// prefixEx is a prefix of expired keys.
	prefixKeyForExpire = "ex:"
	durationForExpire  = 5 * time.Second
	expiredStreamName  = "expired-stream"
	flusherXGroup      = "flushers"
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

func (r *Redis) SubscribeExpiredDataPoints() error {
	// Create consumer group for expired-stream.
	err := r.client.XGroupCreateMkStream(expiredStreamName, flusherXGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return xerrors.Errorf("Could not create consumer group for the stream on redis: %w", err)
	}

	pubsub := r.client.Subscribe("__keyevent@0__:expired")

	// Wait for confirmation that subscription is created before publishing anything.
	if _, err = pubsub.Receive(); err != nil {
		return xerrors.Errorf("Could not receive pub/sub on redis: %w", err)
	}

	log.Println("Waiting expired events")

	ch := pubsub.Channel()
	for msg := range ch {
		metricID := strings.TrimPrefix(msg.Payload, prefixKeyForExpire)
		err := r.client.XAdd(&goredis.XAddArgs{
			Stream: expiredStreamName,
			ID:     "*",
			Values: map[string]interface{}{metricID: ""},
		}).Err()
		if err != nil {
			log.Printf("Could not add message('%s') to stream('%s'): %s\n",
				msg, expiredStreamName, err)
			continue
		}
	}

	return nil
}
