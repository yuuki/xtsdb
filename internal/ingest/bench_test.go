package ingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

func BenchmarkParseGraphiteLine(b *testing.B) {
	now := time.Now()
	const line = "servers.web01.cpu.user;dc=tokyo;rack=a1 12.5 1700000000"
	b.ReportAllocs()
	for b.Loop() {
		if _, ok, err := ParseGraphiteLine(line, now); !ok || err != nil {
			b.Fatal(err)
		}
	}
}

// benchSeries returns n series of 11 labels, and the current time.
func benchSeries(n int) ([]labels.Labels, int64) {
	now := time.Now().UnixMilli()
	out := make([]labels.Labels, n)
	for i := range out {
		out[i] = labels.FromStrings("__name__", "http_requests_total", "cluster", "c1", "env", "prod",
			"instance", fmt.Sprintf("10.0.%d.%d:9100", i/256, i%256), "job", "api", "method", "GET",
			"namespace", "default", "pod", fmt.Sprintf("api-%d", i), "region", "ap-northeast-1", "status", "200")
	}
	return out, now
}

// Prescanning and decoding a request of 1,000 series with 10 samples each,
// the work done before the Writer sees it.
func BenchmarkRemoteWriteDecode(b *testing.B) {
	series, now := benchSeries(1000)
	v1 := &prompb.WriteRequest{}
	st := writev2.NewSymbolTable()
	v2 := &writev2.Request{}
	for _, lset := range series {
		ts1 := prompb.TimeSeries{}
		ts2 := writev2.TimeSeries{LabelsRefs: st.SymbolizeLabels(lset, nil)}
		lset.Range(func(l labels.Label) { ts1.Labels = append(ts1.Labels, prompb.Label{Name: l.Name, Value: l.Value}) })
		for j := range 10 {
			ts1.Samples = append(ts1.Samples, prompb.Sample{Timestamp: now + int64(j)*15_000, Value: float64(j)})
			ts2.Samples = append(ts2.Samples, writev2.Sample{Timestamp: now + int64(j)*15_000, Value: float64(j)})
		}
		v1.Timeseries = append(v1.Timeseries, ts1)
		v2.Timeseries = append(v2.Timeseries, ts2)
	}
	v2.Symbols = st.Symbols()
	b1, err := v1.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b2, err := v2.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	lim := Limits{MaxSeries: 100_000, MaxSamples: 1_000_000}
	for _, c := range []struct {
		name string
		body []byte
		v2   bool
	}{{"v1", b1, false}, {"v2", b2, true}} {
		b.Run(c.name, func(b *testing.B) {
			b.SetBytes(int64(len(c.body)))
			b.ReportAllocs()
			for b.Loop() {
				if err := prescan(c.body, c.v2, lim); err != nil {
					b.Fatal(err)
				}
				var d decoded
				decode := d.v1
				if c.v2 {
					decode = d.v2
				}
				if err := decode(c.body); err != nil || len(d.batch) != 1000 {
					b.Fatal(err)
				}
			}
		})
	}
}

// Writing a batch of 1,000 known series to in-memory stores: validation,
// index cache lookups and the append, without the cost of a backend.
func BenchmarkWriterWrite(b *testing.B) {
	hot := storagetest.NewHot(testLayout, time.Minute)
	w, err := NewWriter(hot, storagetest.NewIndex(), WriterOptions{
		MaxSampleFuture: 10 * time.Minute, MaxSampleAge: 2 * time.Hour,
		IndexCacheSize: 100_000, IndexWriteConcurrency: 4,
	})
	if err != nil {
		b.Fatal(err)
	}
	series, now := benchSeries(1000)
	batch := make([]Series, len(series))
	for i, lset := range series {
		batch[i] = Series{Labels: lset, Samples: []model.Sample{{T: now, V: 1}}}
	}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if res, err := w.Write(ctx, batch); err != nil || res.Accepted != len(batch) {
			b.Fatalf("%+v %v", res, err)
		}
	}
}
