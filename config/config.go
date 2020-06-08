package config

import "time"

const (
	DefaultRedisAddr       = "127.0.0.1:6379"
	DefaultCassandraAddr   = "127.0.0.1:9042"
	DefaultDurationExpires = "1h"
)

type config struct {
	// RedisAddrs is a slice of redis addrs (for redis cluster)
	RedisAddrs []string
	// RedisPubSubAddr is a sharded redis addr for subscribing expired metrics.
	RedisPubSubAddr string
	// CassandraAddrs is a slice of cassandra addrs
	CassandraAddrs []string
	// DurationExpires is duration that datapoints on the memory store expires.
	DurationExpires time.Duration
}

// Config is set from the environment variables and command-line flag values.
var Config = &config{}
