package redis

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	vmstorage "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metrics"
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
	maxBatchSize = 500
)

var (
	metricsExpired = metrics.NewCounter(`xt_metrics_expired_total`)
	metricsFlushed = metrics.NewCounter(`xt_metrics_flushed_total`)
	flushDuration  = metrics.NewSummary(`xt_flush_duration_seconds`)
)

// redisAPI abstratcts goredis.Client and goredis.ClusterClient.
type redisAPI interface {
	Ping() *goredis.StatusCmd
	ScriptLoad(string) *goredis.StringCmd
	Pipeline() goredis.Pipeliner
	Pipelined(func(goredis.Pipeliner) error) ([]goredis.Cmder, error)
	Subscribe(...string) *goredis.PubSub
	XGroupCreateMkStream(string, string, string) *goredis.StatusCmd
	XAdd(*goredis.XAddArgs) *goredis.StringCmd
	XReadGroup(*goredis.XReadGroupArgs) *goredis.XStreamSliceCmd
	XRange(string, string, string) *goredis.XMessageSliceCmd
	Watch(func(*goredis.Tx) error, ...string) error
}

// Redis provides a redis client.
type Redis struct {
	client            redisAPI
	hashScriptAddRows string
}

// New creates a Redis client.
func New() (*Redis, error) {
	addrs := config.Config.RedisAddrs
	var r redisAPI
	switch size := len(addrs); {
	case size == 1:
		r = goredis.NewClient(&goredis.Options{
			Addr:     addrs[0],
			Password: "",
			DB:       0,
		})
	case size > 1:
		r = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:    addrs,
			Password: "",
		})
	default:
		return nil, errors.New("redis addrs are empty")
	}

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

	batchSize := len(mrs)
	if batchSize > maxBatchSize {
		batchSize = maxBatchSize
	}

	evalKeys := make([]string, 0, batchSize)
	evalArgs := make([]interface{}, 0, batchSize*3)

	pipe := r.client.Pipeline()

	// TODO: Remove NaN value
	// scripting the process for batches of items
	for i := 0; i < len(mrs); i += batchSize {
		j := i + batchSize
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

		metricsExpired.Inc()
	}

	return nil
}

// FlushExpiredDataPoints runs a loop of flushing data points
// from Redis to DiskStore.
func (r *Redis) FlushExpiredDataPoints(flushHandler func(string, []goredis.XMessage) error) error {
	if err := r.initExpiredStream(); err != nil {
		return err
	}

	hostname, _ := os.Hostname()
	consumerID := fmt.Sprintf("flusher-%s-%d-%d-%d",
		hostname, os.Getpid(), time.Now().UnixNano(), rand.Int31())

	for {
		startTime := time.Now()

		expiredMetricIDs := []string{}
		expiredStreamIDs := []string{}

		// TODO: launch multiple goroutines
		xstreams, err := r.client.XReadGroup(&goredis.XReadGroupArgs{
			Group:    flusherXGroup,
			Consumer: consumerID,
			Streams:  []string{expiredStreamName, ">"},
			Count:    100,
		}).Result()
		if err != nil {
			if err != goredis.Nil {
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

		if len(expiredMetricIDs) < 1 {
			continue
		}

		// begin transaction flush to cassandra
		metricIDs := make([]string, len(expiredMetricIDs))
		copy(metricIDs, expiredMetricIDs)
		streamIDs := make([]string, len(expiredStreamIDs))
		copy(streamIDs, expiredStreamIDs)

		for _, metricID := range metricIDs {
			xmsgs, err := r.client.XRange(metricID, "-", "+").Result()
			if err != nil {
				return xerrors.Errorf("Could not xrange %v: %w", metricID, err)
			}
			if err := flushHandler(metricID, xmsgs); err != nil {
				return err
			}
		}

		fn := func(tx *goredis.Tx) error {
			// TODO: lua script for xack and del
			_, err := tx.Pipelined(func(pipe goredis.Pipeliner) error {
				if err := pipe.XAck(expiredStreamName, flusherXGroup, streamIDs...).Err(); err != nil {
					return xerrors.Errorf("Could not xack (%s,%s) (%v): %w", expiredStreamName, flusherXGroup, streamIDs, err)
				}
				if err := pipe.XDel(expiredStreamName, streamIDs...).Err(); err != nil {
					return xerrors.Errorf("Could not xdel (%s) (%v): %w", expiredStreamName, streamIDs, err)
				}
				if err := pipe.Del(metricIDs...).Err(); err != nil {
					return xerrors.Errorf("Could not del (%v): %w", metricIDs, err)
				}
				return nil
			})
			if err != nil {
				return xerrors.Errorf("Could not complete to pipeline for deleting old data: %w", err)
			}

			return nil
		}
		// TODO: retry
		if err := r.client.Watch(fn, append(metricIDs, expiredStreamName)...); err != nil {
			log.Printf("failed transaction (%v): %s\n", metricIDs, err)
		}

		flushDuration.UpdateDuration(startTime)
		metricsFlushed.Add(len(streamIDs))
	}
}
