package redis

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"strings"
	"time"

	vmstorage "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	goredis "github.com/go-redis/redis/v7"
	"golang.org/x/xerrors"

	"github.com/yuuki/xtsdb/config"
)

const (
	// prefixEx is a prefix of expired keys.
	prefixKeyForExpire = "ex:"
	expiredStreamName  = "expired-stream"
	flusherXGroup      = "flushers"

	scriptForAddRows = `
		local res
		for i = 1, #KEYS do
			res = redis.call('XADD', KEYS[i], ARGV[i*3-2], '', ARGV[i*3-1]);
			local key = 'ex:'..KEYS[i];
			if redis.call('GET', key) == false then
				res = redis.call('SETEX', key, ARGV[i*3], 1);
			end
		end
		return res
`
	batchSizeAddRows = 310
)

// Redis provides a redis client.
type Redis struct {
	client            *goredis.Client
	hashScriptAddRows string
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

	h := sha1.New()
	io.WriteString(h, scriptForAddRows)
	hash := hex.EncodeToString(h.Sum(nil))

	// register script to redis-server.
	if err := r.ScriptLoad(scriptForAddRows).Err(); err != nil {
		return nil, xerrors.Errorf("could not register script: %w", err)
	}

	return &Redis{client: r, hashScriptAddRows: hash}, nil
}

// AddRows inserts rows into redis-server.
func (r *Redis) AddRows(mrs []vmstorage.MetricRow) error {
	if len(mrs) == 0 {
		return nil
	}

	evalKeys := make([]string, 0, batchSizeAddRows)
	evalArgs := make([]interface{}, 0, batchSizeAddRows)

	pipe := r.client.Pipeline()

	// TODO: Remove NaN value
	// scripting the process for batches of 20 items
	for i := 0; i < len(mrs); i += batchSizeAddRows {
		j := i + batchSizeAddRows
		if j > len(mrs) {
			j = len(mrs)
		}

		for _, row := range mrs[i:j] {
			evalKeys = append(evalKeys, string(row.MetricNameRaw))
			evalArgs = append(evalArgs, int(row.Timestamp), row.Value,
				config.Config.DurationExpires.Seconds())
		}

		err := pipe.EvalSha(r.hashScriptAddRows, evalKeys, evalArgs...).Err()
		if err != nil {
			return xerrors.Errorf("Could not add rows to redis: %w", err)
		}

		// Reset buffer
		evalKeys = evalKeys[:0]
		evalArgs = evalArgs[:0]
	}
	if _, err := pipe.Exec(); err != nil {
		return xerrors.Errorf("Got error of pipeline: %w", err)
	}

	return nil
}

func (r *Redis) initExpiredStream() error {
	// Create consumer group for expired-stream.
	err := r.client.XGroupCreateMkStream(expiredStreamName, flusherXGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return xerrors.Errorf("Could not create consumer group for the stream on redis: %w", err)
	}
	return nil
}

func (r *Redis) SubscribeExpiredDataPoints() error {
	// Create consumer group for expired-stream.
	if err := r.initExpiredStream(); err != nil {
		return err
	}

	pubsub := r.client.Subscribe("__keyevent@0__:expired")

	// Wait for confirmation that subscription is created before publishing anything.
	if _, err := pubsub.Receive(); err != nil {
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

// FlushExpiredDataPoints runs a loop of flushing data points
// from Redis to DiskStore.
func (r *Redis) FlushExpiredDataPoints(flushHandler func(string, []goredis.XMessage) error) error {
	if err := r.initExpiredStream(); err != nil {
		return err
	}

	for {
		expiredMetricIDs := []string{}
		expiredStreamIDs := []string{}

		// TODO: launch multiple goroutines
		xstreams, err := r.client.XReadGroup(&goredis.XReadGroupArgs{
			Group:    flusherXGroup,
			Consumer: "flusher-1",
			Streams:  []string{expiredStreamName, ">"},
			Block:    30 * time.Second,
		}).Result()
		if err != nil {
			if err.Error() != "redis: nil" {
				log.Println(err)
			}
			continue
		}

		for _, xstream := range xstreams {
			for _, xmsg := range xstream.Messages {
				for metricID := range xmsg.Values {
					expiredMetricIDs = append(expiredMetricIDs, metricID)
				}
				expiredStreamIDs = append(expiredStreamIDs, xmsg.ID)
			}
		}
		log.Println(expiredMetricIDs)

		if len(expiredMetricIDs) < 1 {
			continue
		}

		// begin transaction flush to cassandra
		metricIDs := make([]string, len(expiredMetricIDs))
		copy(metricIDs, expiredMetricIDs)
		streamIDs := make([]string, len(expiredStreamIDs))
		copy(streamIDs, expiredStreamIDs)

		go func(metricIDs []string, streamIDs []string) {
			fn := func(tx *goredis.Tx) error {
				for _, metricID := range metricIDs {
					xmsgs, err := tx.XRange(metricID, "-", "+").Result()
					if err != nil {
						return xerrors.Errorf("Could not xrange %v: %w", metricID, err)
					}
					if err := flushHandler(metricID, xmsgs); err != nil {
						return err
					}
				}

				// TODO: lua script for xack and del

				if err := tx.XAck(expiredStreamName, flusherXGroup, streamIDs...).Err(); err != nil {
					return xerrors.Errorf(": %w", err)
				}

				if err := tx.Del(metricIDs...).Err(); err != nil {
					return xerrors.Errorf(": %w", err)
				}
				return nil
			}
			// TODO: retry
			if err := r.client.Watch(fn, metricIDs...); err != nil {
				log.Printf("failed transaction %v: %s", metricIDs, err)
			}
		}(metricIDs, streamIDs)
	}
}
