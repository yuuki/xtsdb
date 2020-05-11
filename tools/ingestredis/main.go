package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	goredis "github.com/go-redis/redis/v7"
)

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
			_, err := client.Set(fmt.Sprintf("ex:%s", stream), true, 10*time.Second).Result()
			if err != nil {
				log.Printf("Could not set stream: %s", err)
				continue
			}
			fmt.Printf("Set expire to stream '%s'\n", stream)
		}
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
	}
}
