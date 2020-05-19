package config

import "time"

const (
	DefaultDurationExpires = 600 * time.Second
)

type config struct {
	// DurationExpires is duration that datapoints on the memory store expires.
	DurationExpires time.Duration
}

// Config is set from the environment variables and command-line flag values.
var Config = &config{}
