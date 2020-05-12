package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v7"
)

const (
	expiredStream = "expired-stream"
	prefixEx      = "ex:"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {
	var addr = flag.String("addr", "localhost:6379", "addr:port to redis server")
	var db = flag.Int("db", 0, "redis database")

	client := goredis.NewClient(&goredis.Options{
		Addr:     *addr,
		Password: "",
		DB:       *db,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

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
		// TODO: consumer group
		// reading from expired-stream
		// https://github.com/antirez/redis/issues/5543
		startID := "$"
		expiredMetricIDs := []string{}

	again:
		xstreams, err := client.XRead(&goredis.XReadArgs{
			Streams: []string{expiredStream, startID},
			Block:   0,
		}).Result()
		if err != nil {
			log.Fatal(err)
		}

		for _, xstream := range xstreams {
			for _, xmsg := range xstream.Messages {
				for metricID := range xmsg.Values {
					expiredMetricIDs = append(expiredMetricIDs, metricID)
				}
				startID = xmsg.ID
			}
		}
		log.Println(expiredMetricIDs)

		if len(expiredMetricIDs) < 1 {
			goto again
		}

		// begin transaction flush to cassandra
		metricIDs := expiredMetricIDs
		go func(metricIDs []string) {
			fn := func(tx *goredis.Tx) error {
				for _, metricID := range metricIDs {
					xmsgs, err := tx.XRange(metricID, "-", "+").Result()
					if err != nil {
						return err
					}
					log.Printf("Flush datapoints to cassandra: %s %v\n", metricID, xmsgs)
				}
				if err := tx.Del(metricIDs...).Err(); err != nil {
					return err
				}
				return nil
			}
			// TODO: retry
			if err := client.Watch(fn, metricIDs...); err != nil {
				log.Println(err)
			}
		}(metricIDs)

		// zero clear
		expiredMetricIDs = expiredMetricIDs[:0]

		goto again
	}()

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
