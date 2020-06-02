package config

import "time"

const (
	DefaultRedisAddr       = "127.0.0.1:6379"
	DefaultDurationExpires = "1h"
)

type config struct {
	// RedisAddrs is a slice of redis addrs (for redis cluster)
	RedisAddrs []string
	// DurationExpires is duration that datapoints on the memory store expires.
	DurationExpires time.Duration
}

// Config is set from the environment variables and command-line flag values.
var Config = &config{}
