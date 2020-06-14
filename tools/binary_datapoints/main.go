package main

import (
	"encoding/binary"
	"log"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	xxhash "github.com/cespare/xxhash/v2"
	goredis "github.com/go-redis/redis/v7"
)

func main() {
	metricName := "cpu.usage_user;hostname=host_0;region=ap-northeast-1;datacenter=ap-northeast-1a;rack=72;os=Ubuntu16.10;arch=x86;team=CHI;service=10;service_version=0;service_environment=test"

	mid := xxhash.Sum64String(metricName)
	instanceID := xxhash.Sum64String("host_0")

	metricID := stringFromUint64(mid) +
		":{" + stringFromUint64(instanceID) + "}"
	log.Println(metricID)

	red := goredis.NewClient(&goredis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
	})
	red.FlushAll()

	red.Append("ts:"+metricID, metricID)
	id, err := red.Get("ts:" + metricID).Result()
	if err != nil {
		log.Println(err)
	}
	log.Println(id)
	id2, err := red.Get("ts:" + id).Result()
	if err != nil {
		log.Println(err)
	}
	log.Println(id2)
}

func stringFromUint64(id uint64) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return bytesutil.ToUnsafeString(b)
}
