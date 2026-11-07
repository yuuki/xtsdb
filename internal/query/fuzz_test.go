package query

import (
	"math"
	"strconv"
	"testing"
)

// The time parameters of the query API are untrusted. Parsed times stay
// within the clamped range, so that converting them to milliseconds never
// overflows.
func FuzzParseTime(f *testing.F) {
	for _, s := range []string{"1700000000", "1700000000.123", "2023-11-14T22:13:20Z", "1e300", "-1e300", "-Inf", "NaN",
		"0x1p62", "9223372036854775807", minTimeFormatted, maxTimeFormatted, "10000-01-01T00:00:00Z", ""} {
		f.Add(s)
	}
	const most = int64(maxSeconds)*1000 + 999
	f.Fuzz(func(t *testing.T, s string) {
		tm, err := parseTime(s)
		if err != nil {
			return
		}
		if ms := tm.UnixMilli(); ms < -most || ms > most {
			t.Fatalf("parseTime(%q) = %v (%d ms), beyond the clamped range", s, tm, ms)
		}
	})
}

// Durations given in seconds keep their sign: a value too large for a
// time.Duration is an error, never a wrapped-around negative duration.
func FuzzParseDuration(f *testing.F) {
	for _, s := range []string{"15", "1.5", "5m", "1d", "-1", "1e-10", "9223372036.854775807", "9223372036.854775808",
		"-9223372036.854775808", "1e300", "NaN", "Inf", "x", "0x1p33"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		d, err := parseDuration(s)
		if err != nil {
			return
		}
		if v, ferr := strconv.ParseFloat(s, 64); ferr == nil && (math.Signbit(v) != (d < 0) && d != 0) {
			t.Fatalf("parseDuration(%q) = %v, with the sign of %v flipped", s, d, v)
		}
	})
}
