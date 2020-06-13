package redis

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/metrics"
	goredis "github.com/go-redis/redis/v7"
	"golang.org/x/sync/errgroup"
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
)

var (
	metricsExpired     = metrics.NewCounter(`xt_metrics_expired_total`)
	metricsFlushed     = metrics.NewCounter(`xt_metrics_flushed_total`)
	flushDuration      = metrics.NewSummary(`xt_flush_duration_seconds`)
	insertRowsDuration = metrics.NewSummary(`xt_insert_rows_duration_seconds`)

	scriptForAddRows = `
		local res
		local maxKeyLen = %d
		local expiredStreamKey = '%s'
		local expiredKeyPrefix = '%s'
       	for i = 1, #KEYS do
			local ek = expiredKeyPrefix..KEYS[i]
			res = redis.call('APPEND', KEYS[i], ARGV[i*2-1])
			if redis.call('STRLEN', KEYS[i]) >= maxKeyLen then
				redis.call('XADD', expiredStreamKey, '*', KEYS[i], "")
				redis.call('SET', ek, 1, "EX", ARGV[i*2])
			end
			if redis.call('EXISTS', ek) == 0 then
				redis.call('SET', ek, 1, "EX", ARGV[i*2])
			end
		end
		return res
	`
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
	Get(key string) *goredis.StringCmd
	Del(...string) *goredis.IntCmd
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

func isCluster(addr string) (bool, error) {
	red := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: "",
	})
	v, err := red.Info("CLUSTER").Result()
	if err != nil {
		return false, xerrors.Errorf("Could not get INFO CLUSTER")
	}
	if kv := strings.SplitN(v, ":", 2); len(kv) > 1 {
		if kv[1] == "1" {
			return true, nil
		}
		return false, nil
	} else {
		return false, xerrors.Errorf("invalid format of INFO CLUSTER: %q", kv)
	}
	panic("not reachable")
}

// New creates a Redis client.
func New(addrs []string) (*Redis, error) {
	var (
		r redisAPI
	)

	cluster, err := isCluster(addrs[0])
	if err != nil {
		return nil, err
	}
	if cluster {
		r = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:      addrs,
			Password:   "",
			MaxRetries: 2,
		})
	} else {
		r = goredis.NewClient(&goredis.Options{
			Addr:       addrs[0],
			Password:   "",
			DB:         0,
			MaxRetries: 2,
		})
	}

	if err := r.Ping().Err(); err != nil {
		return nil, xerrors.Errorf("could not ping: %w", err)
	}

	var (
		script           string
		selfShardID      int
		expiredStreamKey string = expiredStreamName
		maxSeriesLen     int    = (8 + 8) * config.Config.MaxSeriesLength // datapoint = int64 + float64
	)

	if rcc, ok := r.(*goredis.ClusterClient); ok {
		// TODO: rename RedisPubSubAddr
		var err error
		selfShardID, err = getSelfShardID(rcc, addrs[0])
		if err != nil {
			return nil, err
		}

		expiredStreamKey = fmt.Sprintf("%s:%d", expiredStreamName, selfShardID)
		script = fmt.Sprintf(scriptForAddRows, maxSeriesLen, expiredStreamKey, prefixKeyForExpire)

		// register script to redis-server.
		err = rcc.ForEachMaster(func(client *goredis.Client) error {
			return client.ScriptLoad(script).Err()
		})
		if err != nil {
			return nil, xerrors.Errorf(
				"could not register script to cluster masters: %w", err)
		}
	} else {
		script = fmt.Sprintf(scriptForAddRows, maxSeriesLen, expiredStreamKey, prefixKeyForExpire)
		// register script to redis-server.
		if err := r.ScriptLoad(script).Err(); err != nil {
			return nil, xerrors.Errorf("could not register script: %w", err)
		}
	}

	h := sha1.New()
	io.WriteString(h, script)
	hash := hex.EncodeToString(h.Sum(nil))

	red := &Redis{
		client:               r,
		cluster:              cluster,
		hashScriptAddRows:    hash,
		selfShardID:          selfShardID,
		selfExpiredStreamKey: expiredStreamKey,
	}
	return red, nil
}

func (r *Redis) redisClient(addr string) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
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

func getMetricID(metricName string) uint64 {
	h := fnv.New64a()
	h.Write(bytesutil.ToUnsafeBytes(metricName))
	return h.Sum64()
}

// AddRows inserts rows into redis-server.
func (r *Redis) AddRows(mrs model.MetricRows) error {
	if len(mrs) == 0 {
		return nil
	}
	startTime := time.Now()

	eseconds := config.Config.DurationExpires.Seconds()
	ebMap := make(map[string]*evalBuffer, len(mrs))
	for label := range mrs {
		rows := mrs[label]
		if len(rows) < 1 {
			continue
		}
		eb := &evalBuffer{
			keys: make([]string, 0, len(rows)),
			args: make([]interface{}, 0, len(rows)*2),
		}
		datapoints := make([]byte, len(rows)*2*8) // 2 -> (timestamp, value) 8 -> bytes of int64 or float64
		for i := range rows {
			row := &rows[i]

			dp := datapoints[i*2*8 : (i+1)*2*8]
			binary.BigEndian.PutUint64(dp[0:8], *(*uint64)(unsafe.Pointer(&row.Timestamp)))
			binary.BigEndian.PutUint64(dp[8:16], math.Float64bits(row.Value))

			id := getMetricID(row.MetricName)
			ttl := int(eseconds + math.Mod(float64(id), eseconds))

			eb.keys = append(eb.keys, row.MetricName)
			eb.args = append(eb.args, bytesutil.ToUnsafeString(dp), ttl)
		}
		ebMap[label] = eb
	}

	// TODO: Remove NaN value
	cmds, err := r.client.Pipelined(func(pipe goredis.Pipeliner) error {
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
		var (
			firstErr       error
			firstCmdString string
		)
		for _, cmd := range cmds {
			if err := cmd.Err(); err != nil {
				firstErr = err
				firstCmdString = cmd.String()
				break
			}
		}
		return xerrors.Errorf("Got error of redis pipeline (%q): %w", firstCmdString, firstErr)
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
func (r *Redis) SubscribeExpiredDataPoints(addr string) error {
	// Create consumer group for expired-stream.
	if err := r.initExpiredStream(); err != nil {
		return err
	}

	// Do not use ClusterClient because goredis routes based on hashing string of keyspace notifications channel.
	pubsub := r.redisClient(addr).Subscribe(expiredEventChannel)

	// Wait for confirmation that subscription is created before publishing anything.
	if _, err := pubsub.Receive(); err != nil {
		return xerrors.Errorf("Could not receive pub/sub on redis: %w", err)
	}

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
func (r *Redis) FlushExpiredDataPoints(flushHandler func(map[string][]byte) error) error {
	if err := r.initExpiredStream(); err != nil {
		return err
	}

	consumerID := generateConsumerID()

	log.Printf("Subscribing '%s' as ('%s','%s')",
		r.selfExpiredStreamKey, flusherXGroup, consumerID)

	for {
		startTime := time.Now()

		expiredMetricIDs := []string{}
		expiredStreamIDs := []string{}

		xstreams, err := r.client.XReadGroup(&goredis.XReadGroupArgs{
			Group:    flusherXGroup,
			Consumer: consumerID,
			Streams:  []string{r.selfExpiredStreamKey, ">"},
			Count:    20,
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

		vals, err := r.client.Pipelined(func(pipe goredis.Pipeliner) error {
			for _, metricID := range expiredMetricIDs {
				// TODO: mget
				pipe.Get(metricID)
			}
			return nil
		})
		if err != nil {
			log.Printf("Could not pipeline get: %+v", err)
			continue
		}
		mapRows := make(map[string][]byte, len(expiredMetricIDs))
		for i, val := range vals {
			metricID := expiredMetricIDs[i]
			v := val.(*goredis.StringCmd).Val()
			mapRows[metricID] = append(mapRows[metricID], bytesutil.ToUnsafeBytes(v)...)
		}
		if err := flushHandler(mapRows); err != nil {
			log.Printf("%+v", err)
			continue
		}

		eg := errgroup.Group{}

		eg.Go(func() error {
			// TODO: retry
			err = r.client.Watch(func(tx *goredis.Tx) error {
				// TODO: lua script for xack and del
				_, err := tx.Pipelined(func(pipe goredis.Pipeliner) error {
					pipe.XAck(r.selfExpiredStreamKey, flusherXGroup, expiredStreamIDs...)
					pipe.XDel(r.selfExpiredStreamKey, expiredStreamIDs...)
					return nil
				})
				return err
			}, r.selfExpiredStreamKey)
			if err != nil {
				return xerrors.Errorf("failed transaction (%v...): %w\n", expiredMetricIDs[0], err)
			}
			return nil
		})

		eg.Go(func() error {
			// TODO: handling in case of delete failure
			mapMetricIDs := groupMetricIDsByHashTag(expiredMetricIDs)
			_, err = r.client.Pipelined(func(pipe goredis.Pipeliner) error {
				for _, ids := range mapMetricIDs {
					if len(ids) < 1 {
						continue
					}
					pipe.Unlink(ids...)
				}
				return nil
			})
			if err != nil {
				return xerrors.Errorf("Could not complete to pipeline for deleteing old metrics (%v...): %w", expiredMetricIDs[0], err)
			}
			return nil
		})

		if err := eg.Wait(); err != nil {
			log.Printf("%+v", err)
			continue
		}

		flushDuration.UpdateDuration(startTime)
		metricsFlushed.Add(len(expiredStreamIDs))
	}
}

func generateConsumerID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("flusher-%s-%d-%d-%d",
		hostname, os.Getpid(), time.Now().UnixNano(), rand.Int31())
}

func groupMetricIDsByHashTag(metricIDs []string) map[string][]string {
	mapMetricID := make(map[string][]string)
	for _, metricID := range metricIDs {
		s := strings.Index(metricID, "{")
		if s == -1 {
			log.Printf("%s should be contained '{'", metricID)
			continue
		}
		e := strings.Index(metricID, "}")
		if e == -1 {
			log.Printf("%s should be contained '}'", metricID)
			continue
		}
		if s > e {
			log.Printf("%s[%d:%d] bounds out of range", metricID, s, e)
			continue
		}
		key := metricID[s+1 : e]
		mapMetricID[key] = append(mapMetricID[key], metricID)
	}
	return mapMetricID
}
