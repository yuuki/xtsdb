package model

// MetricRow is a metric to insert into storage.
type MetricRow struct {
	MetricName string

	Timestamp int64
	Value     float64
}

type MetricRows map[string][]MetricRow
