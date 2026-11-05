package config

import (
	"fmt"
	"time"

	"github.com/prometheus/common/model"
)

// Duration is a time.Duration that can be decoded from Prometheus-style
// duration strings such as "30s", "1h", "90d" or "2w".
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(model.Duration(d).String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := model.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(text), err)
	}
	*d = Duration(v)
	return nil
}

// String implements fmt.Stringer.
func (d Duration) String() string { return model.Duration(d).String() }
