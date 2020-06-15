package model

import (
	"encoding/binary"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	xxhash "github.com/cespare/xxhash/v2"
)

// MetricRow is a metric to insert into storage.
type MetricRow struct {
	MetricName string
	MetricID   string

	Timestamp int64
	Value     float64
}

type MetricRows map[string][]MetricRow

func (mr *MetricRow) MetricNameHash() uint64 {
	return xxhash.Sum64String(mr.MetricName)
}

func (mr *MetricRow) SetMetricID(instance string) {
	mid := xxhash.Sum64String(mr.MetricName)
	// redis hash tag
	mr.MetricID = "{" + instance + ";}:" + stringFromUint64(mid)
}

func stringFromUint64(id uint64) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return bytesutil.ToUnsafeString(b)
}
