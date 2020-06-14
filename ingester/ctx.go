/* This file contains some modifications to the source code of VictoriaMetrics.
https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9e8733ff65ac9a540b0d8367d97f501a965498f0/app/vminsert/common/insert_ctx.go
*/
package ingester

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"

	"github.com/yuuki/xtsdb/storage"
	"github.com/yuuki/xtsdb/storage/model"
)

// InsertCtx contains common bits for data points insertion.
type InsertCtx struct {
	Labels []prompb.Label

	mrs model.MetricRows
}

// Reset resets ctx for future fill with rowsLen rows.
func (ctx *InsertCtx) Reset(rowsLen int) {
	for _, label := range ctx.Labels {
		label.Name = nil
		label.Value = nil
	}
	ctx.Labels = ctx.Labels[:0]

	// for i := range ctx.mrs {
	// 	delete(ctx.mrs, i)
	// }
}

const (
	hostLabelName = "hostname"
)

func (ctx *InsertCtx) concatMetricName(labels []prompb.Label) (string, *prompb.Label) {
	var (
		metricName     string
		metricBaseName string
		hostLabel      *prompb.Label
	)

	for i := range labels {
		label := &labels[i]
		labelName := bytesutil.ToUnsafeString(label.Name)
		if labelName == "" {
			metricBaseName = bytesutil.ToUnsafeString(label.Value)
			continue
		}
		if labelName == hostLabelName {
			hostLabel = label
		}
		metricName += labelName + "=" + bytesutil.ToUnsafeString(label.Value) + ";"
	}

	return metricBaseName + ";" + metricName, hostLabel
}

// WriteDataPoint writes (timestamp, value) with the given prefix and labels into ctx buffer.
func (ctx *InsertCtx) WriteDataPoint(prefix []byte, labels []prompb.Label, timestamp int64, value float64) {
	metricName, hostLabel := ctx.concatMetricName(labels)
	ctx.addRow(metricName, hostLabel, timestamp, value)
}

func (ctx *InsertCtx) addRow(metricName string, hostLabel *prompb.Label, timestamp int64, value float64) {
	hostName := string(hostLabel.Value) // copy safe
	mrs := ctx.mrs[hostName]
	if cap(mrs) > len(mrs) {
		mrs = mrs[:len(mrs)+1]
	} else {
		mrs = append(mrs, model.MetricRow{})
	}
	mr := &mrs[len(mrs)-1]
	ctx.mrs[hostName] = mrs
	mr.MetricName = metricName
	mr.Timestamp = timestamp
	mr.Value = value
	mr.SetMetricID(hostName)
}

// AddLabel adds (name, value) label to ctx.Labels.
//
// name and value must exist until ctx.Labels is used.
func (ctx *InsertCtx) AddLabel(name, value string) {
	labels := ctx.Labels
	if cap(labels) > len(labels) {
		labels = labels[:len(labels)+1]
	} else {
		labels = append(labels, prompb.Label{})
	}
	label := &labels[len(labels)-1]

	// Do not copy name and value contents for performance reasons.
	// This reduces GC overhead on the number of objects and allocations.
	label.Name = bytesutil.ToUnsafeBytes(name)
	label.Value = bytesutil.ToUnsafeBytes(value)

	ctx.Labels = labels
}

// FlushBufs flushes buffered rows to the underlying storage.
func (ctx *InsertCtx) FlushBufs() error {
	if err := storage.AddRows(ctx.mrs); err != nil {
		return &httpserver.ErrorWithStatusCode{
			Err:        fmt.Errorf("cannot store metrics: %s", err),
			StatusCode: http.StatusServiceUnavailable,
		}
	}
	return nil
}
