package model

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// Canonicalizing and hashing the labels of an incoming series, done for
// every series of every write.
func BenchmarkCanonicalizeID(b *testing.B) {
	lset := labels.FromStrings("__name__", "http_requests_total", "cluster", "c1", "env", "prod",
		"instance", "10.0.0.1:9100", "job", "api", "method", "GET", "namespace", "default",
		"pod", "api-7d9f8b6c5-x2x4z", "region", "ap-northeast-1", "status", "200")
	b.ReportAllocs()
	for b.Loop() {
		out, err := Canonicalize(lset)
		if err != nil {
			b.Fatal(err)
		}
		_ = ID(out)
		_ = Check(out)
	}
}
