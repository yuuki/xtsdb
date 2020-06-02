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
		local res = redis.call('XADD', KEYS[1], ARGV[1], '', ARGV[2]);
		if redis.call('GET', KEYS[2]) == false then
			res = redis.call('SETEX', KEYS[2], ARGV[3], 1);
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
	rcc, ok := r.(*goredis.ClusterClient)
	if ok {
		err := rcc.ForEachMaster(func(client *goredis.Client) error {
			return client.ScriptLoad(scriptForAddRows).Err()
		})
		if err != nil {
			return nil, xerrors.Errorf(
				"could not register script to cluster masters: %w", err)
		}
	} else {
		if err := r.ScriptLoad(scriptForAddRows).Err(); err != nil {
			return nil, xerrors.Errorf("could not register script: %w", err)
		}
	}

	return &Redis{client: r, hashScriptAddRows: hash}, nil
}

// AddRows inserts rows into redis-server.
func (r *Redis) AddRows(mrs []vmstorage.MetricRow) error {
	if len(mrs) == 0 {
		return nil
	}

	// TODO: Remove NaN value
	_, err := r.client.Pipelined(func(pipe goredis.Pipeliner) error {
		for _, row := range mrs {
			name := "{" + string(row.MetricNameRaw) + "}" // redis hash tag
			evalKeys := []string{name, prefixKeyForExpire + name}
			evalArgs := []interface{}{int(row.Timestamp), row.Value,
				config.Config.DurationExpires.Seconds()}
			err := pipe.EvalSha(r.hashScriptAddRows, evalKeys, evalArgs...).Err()
			if err != nil {
				return xerrors.Errorf("Could not add rows to redis: %w", err)
			}
		}
		return nil
	})
	if err != nil {
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
