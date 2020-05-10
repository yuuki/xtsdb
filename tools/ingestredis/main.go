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

	client := goredis.NewClient(&goredis.Options{
		Addr:     *addr,
		Password: "",
		DB:       0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

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
		ok, err := client.Expire(stream, 10*time.Second).Result()
		if err != nil {
			log.Printf("Could not expire stream: %s", err)
			continue
		}
		if !ok {
			log.Printf("Return not ok after sending expire: %s", err)
			continue
		}
		fmt.Printf("Set expire to stream '%s'\n", stream)
	}
}
