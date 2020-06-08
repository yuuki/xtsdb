package redis

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	goredis "github.com/go-redis/redis/v7"
	"golang.org/x/xerrors"

	"github.com/yuuki/xtsdb/config"
	"github.com/yuuki/xtsdb/storage/model"
)

const (
	// prefixEx is a prefix of expired keys.
	prefixKeyForExpire  = "ex:"
	expiredStreamName   = "expired-stream"
	flusherXGroup       = "flushers"
	expiredEventChannel = "__keyevent@0__:expired"

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
	metricsExpired     = metrics.NewCounter(`xt_metrics_expired_total`)
	metricsFlushed     = metrics.NewCounter(`xt_metrics_flushed_total`)
	flushDuration      = metrics.NewSummary(`xt_flush_duration_seconds`)
	insertRowsDuration = metrics.NewSummary(`xt_insert_rows_duration_seconds`)
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
	EvalSha(string, []string, ...interface{}) *goredis.Cmd
}

// Redis provides a redis client.
type Redis struct {
	client               redisAPI
	cluster              bool
	hashScriptAddRows    string
	selfShardID          int
	selfExpiredStreamKey string
}

// New creates a Redis client.
func New(addrs []string, cluster bool) (*Redis, error) {
	var (
		r           redisAPI
		selfShardID int
	)

	if cluster {
		r = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:    addrs,
			Password: "",
		})
	} else {
		r = goredis.NewClient(&goredis.Options{
			Addr:     addrs[0],
			Password: "",
			DB:       0,
		})
	}

	if err := r.Ping().Err(); err != nil {
		return nil, xerrors.Errorf("could not ping: %w", err)
	}

	h := sha1.New()
	io.WriteString(h, scriptForAddRows)
	hash := hex.EncodeToString(h.Sum(nil))

	if rcc, ok := r.(*goredis.ClusterClient); ok {
		// register script to redis-server.
		err := rcc.ForEachMaster(func(client *goredis.Client) error {
			return client.ScriptLoad(scriptForAddRows).Err()
		})
		if err != nil {
			return nil, xerrors.Errorf(
				"could not register script to cluster masters: %w", err)
		}
		// TODO: rename RedisPubSubAddr
		selfShardID, err = getSelfShardID(rcc, addrs[0])
		if err != nil {
			return nil, err
		}
	} else {
		// register script to redis-server.
		if err := r.ScriptLoad(scriptForAddRows).Err(); err != nil {
			return nil, xerrors.Errorf("could not register script: %w", err)
		}
	}

	return &Redis{
		client:               r,
		cluster:              cluster,
		hashScriptAddRows:    hash,
		selfShardID:          selfShardID,
		selfExpiredStreamKey: fmt.Sprintf("%s:%d", expiredStreamName, selfShardID),
	}, nil
}

// getSelfShardID gets config-epoch as a shard ID.
func getSelfShardID(rcc *goredis.ClusterClient, selfAddr string) (int, error) {
	res, err := rcc.ClusterNodes().Result()
	if err != nil {
		return -1, xerrors.Errorf(
			"could not get cluster nodes: %w", err)
	}
	/** An example of output of CLUSTER NODES
	01808c924272decdda8efdcc025c2ab34b17c049 10.0.0.210:6379@16379 master - 0 1591620071014 1 connected 0-4095
	44d48a123e5cecb5f62027cc74eaa6c356d352d5 10.0.0.212:6379@16379 master - 0 1591620070000 3 connected 8192-12287
	e95321c1c100fab5a40595d7683d5bf9550a9564 10.0.0.213:6379@16379 master - 0 1591620069974 4 connected 12288-16383
	aa7c1f537643f99c68f0cd4e4fe9d8cfff8f14d6 10.0.0.211:6379@16379 myself,master - 0 1591620069000 2 connected 4096-8191
	**/
	for _, line := range strings.Split(res, "\n") {
		if strings.Contains(line, selfAddr) {
			s := strings.Split(line, " ")[6]
			configEpoch, err := strconv.Atoi(s)
			if err != nil {
				return -1, xerrors.Errorf(
					"%q should be integer: %w", s, err)
			}
			// TODO: Dealing with the case where configEpoch is incremented
			// when a slave is promoted.
			return configEpoch, nil
		}
	}
	return -1, xerrors.Errorf("not found selfAddr(%s) from CLUSTER NODES", selfAddr)
}

type evalBuffer struct {
	keys []string
	args []interface{}
}

// AddRows inserts rows into redis-server.
func (r *Redis) AddRows(mrs model.MetricRows) error {
	if len(mrs) == 0 {
		return nil
	}
	startTime := time.Now()

	ebMap := make(map[string]*evalBuffer, len(mrs))
	for label := range mrs {
		rows := mrs[label]
		if len(rows) < 1 {
			continue
		}
		eb := &evalBuffer{
			keys: make([]string, 0, len(rows)),
			args: make([]interface{}, 0, len(rows)*3),
		}
		for i := range rows {
			row := &rows[i]
			eb.keys = append(eb.keys, row.MetricName)
			// TODO: fix int cast
			eb.args = append(eb.args, int(row.Timestamp), row.Value,
				config.Config.DurationExpires.Seconds())
		}
		ebMap[label] = eb
	}

	// TODO: Remove NaN value
	_, err := r.client.Pipelined(func(pipe goredis.Pipeliner) error {
		for _, eb := range ebMap {
			err := pipe.EvalSha(r.hashScriptAddRows, eb.keys, eb.args...).Err()
			if err != nil {
				return xerrors.Errorf("Could not add rows to redis (keylen:%d, arglen:%v): %w",
					len(eb.keys), len(eb.args), err)
			}
		}
		return nil
	})
	if err != nil {
		return xerrors.Errorf("Got error of redis pipeline: %w", err)
	}

	insertRowsDuration.UpdateDuration(startTime)
	return nil
}

func (r *Redis) initExpiredStream() error {
	// Create consumer group for expired-stream.
	err := r.client.XGroupCreateMkStream(r.selfExpiredStreamKey, flusherXGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return xerrors.Errorf("Could not create consumer group for the stream (%s) on redis: %w", r.selfExpiredStreamKey, err)
	}
	return nil
}

// SubscribeExpiredDataPoints subscribes expired data points and inserts 'expired-stream'.
func (r *Redis) SubscribeExpiredDataPoints() error {
	// Create consumer group for expired-stream.
	if err := r.initExpiredStream(); err != nil {
		return err
	}

	pubsub := r.client.Subscribe(expiredEventChannel)

	// Wait for confirmation that subscription is created before publishing anything.
	if _, err := pubsub.Receive(); err != nil {
		return xerrors.Errorf("Could not receive pub/sub on redis: %w", err)
	}

	log.Println("Waiting expired events")

	ch := pubsub.Channel()
	for msg := range ch {
		metricID := strings.TrimPrefix(msg.Payload, prefixKeyForExpire)
		err := r.client.XAdd(&goredis.XAddArgs{
			Stream: r.selfExpiredStreamKey,
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

	consumerID := generateConsumerID()

	for {
		startTime := time.Now()

		expiredMetricIDs := []string{}
		expiredStreamIDs := []string{}

		xstreams, err := r.client.XReadGroup(&goredis.XReadGroupArgs{
			Group:    flusherXGroup,
			Consumer: consumerID,
			Streams:  []string{r.selfExpiredStreamKey, ">"},
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
				if err := pipe.XAck(r.selfExpiredStreamKey, flusherXGroup, streamIDs...).Err(); err != nil {
					return xerrors.Errorf("Could not xack (%s,%s) (%v): %w", expiredStreamName, flusherXGroup, streamIDs, err)
				}
				if err := pipe.XDel(r.selfExpiredStreamKey, streamIDs...).Err(); err != nil {
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
		if err := r.client.Watch(fn, append(metricIDs, r.selfExpiredStreamKey)...); err != nil {
			log.Printf("failed transaction (%v): %s\n", metricIDs, err)
		}

		flushDuration.UpdateDuration(startTime)
		metricsFlushed.Add(len(streamIDs))
	}
}

func generateConsumerID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("flusher-%s-%d-%d-%d",
		hostname, os.Getpid(), time.Now().UnixNano(), rand.Int31())
}
