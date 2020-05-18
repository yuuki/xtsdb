/* Before you execute the program, Launch `cqlsh` and execute:
cqlsh --execute="create keyspace xtsdb with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
cqlsh --keyspace xtsdb --file assets/scheme/series_cassandra.sql
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v7"
	"github.com/gocql/gocql"
)

const (
	expiredStream = "expired-stream"
	prefixEx      = "ex:"
	flusherXGroup = "flushers"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {
	var redisAddr = flag.String("redis-addr", "127.0.0.1:6379", "addr:port to redis server")
	var redisDB = flag.Int("redis-db", 0, "redis database")
	var cassandraAddr = flag.String("cassandra-addr", "127.0.0.1:9042", "addr:port to cassandra server")
	var cassandraKeyspace = flag.String("cassandra-keyspace", "xtsdb", "cassandra keyspace")

	// Setup redis client
	client := goredis.NewClient(&goredis.Options{
		Addr:     *redisAddr,
		Password: "",
		DB:       *redisDB,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	// Setup cassandra client
	cluster := gocql.NewCluster(*cassandraAddr)
	cluster.Keyspace = *cassandraKeyspace
	cluster.Consistency = gocql.Any
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// ingester
	go func() {
		metrics := map[string][]struct {
			Timestamp int64
			Value     float64
		}{
			"host001.cpu.idle": {
				{Timestamp: 1589105560, Value: 0.1},
				{Timestamp: 1589105561, Value: 0.2},
				{Timestamp: 1589105562, Value: 0.3},
			},
			"host001.cpu.user": {
				{Timestamp: 1589105560, Value: 1.1},
				{Timestamp: 1589105561, Value: 1.2},
				{Timestamp: 1589105562, Value: 1.3},
			},
		}

		for stream, datapoints := range metrics {
			for _, datapoint := range datapoints {
				// TODO: redis lua scripting
				_, err := client.XAdd(&goredis.XAddArgs{
					Stream: stream,
					ID:     fmt.Sprintf("%d", datapoint.Timestamp),
					Values: map[string]interface{}{"": datapoint.Value},
				}).Result()
				if err != nil {
					log.Printf("Could not add stream: %s", err)
					continue
				}
			}

			// TODO: check expired and set
			key := prefixEx + stream
			_, err := client.Set(key, true, 5*time.Second).Result()
			if err != nil {
				log.Printf("Could not set stream: %s", err)
				continue
			}
			fmt.Printf("Set expire to key '%s'\n", key)
		}
	}()

	// flusher
	go func() {
		// reading from expired-stream
		// https://github.com/antirez/redis/issues/5543

		for {
			expiredMetricIDs := []string{}
			expiredStreamIDs := []string{}

			// TODO: launch multiple goroutines
			xstreams, err := client.XReadGroup(&goredis.XReadGroupArgs{
				Group:    flusherXGroup,
				Consumer: "flusher-1",
				Streams:  []string{expiredStream, ">"},
				Count:    100,
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
							log.Println(err)
							return err
						}
						// TODO: Flush to cassandra
						log.Printf("Flush datapoints to cassandra: %s %v\n", metricID, xmsgs)
						for _, xmsg := range xmsgs {
							t, err := strconv.ParseInt(strings.TrimSuffix(xmsg.ID, "-0"), 10, 64)
							if err != nil {
								log.Println(err)
								return err
							}

							timestamp := time.Unix(t, 0)

							for _, v := range xmsg.Values {
								val, err := strconv.ParseFloat(v.(string), 64)
								if err != nil {
									log.Println(err)
									return err
								}
								err = session.Query(`
									INSERT INTO datapoint (metric_id, timestamp, value)
									VALUES (?, ?, ?) `, metricID, timestamp, val,
								).Exec()
								if err != nil {
									log.Println(err)
									return err
								}
							}
						}
					}

					// TODO: lua script for xack and del

					if err := tx.XAck(expiredStream, flusherXGroup, streamIDs...).Err(); err != nil {
						log.Println(err)
						return err
					}

					if err := tx.Del(metricIDs...).Err(); err != nil {
						log.Println(err)
						return err
					}
					return nil
				}
				// TODO: retry
				if err := client.Watch(fn, metricIDs...); err != nil {
					log.Println(err)
				}
			}(metricIDs, streamIDs)
		}
	}()

	// Create consumer group for expired-stream.
	err = client.XGroupCreateMkStream(expiredStream, flusherXGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Fatalln(err)
	}

	pubsub := client.Subscribe("__keyevent@0__:expired")

	// Wait for confirmation that subscription is created before publishing anything.
	if _, err = pubsub.Receive(); err != nil {
		log.Fatal(err)
	}

	log.Println("Waiting expired events")

	ch := pubsub.Channel()
	for msg := range ch {
		fmt.Println(msg.Channel, msg.Payload)

		metricID := strings.TrimPrefix(msg.Payload, prefixEx)
		err := client.XAdd(&goredis.XAddArgs{
			Stream: expiredStream,
			ID:     "*",
			Values: map[string]interface{}{metricID: ""},
		}).Err()
		if err != nil {
			log.Printf("Could not add stream: %s", err)
			continue
		}
	}
}
