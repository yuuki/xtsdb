package ingest

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// Limits bounds the size of a single remote write request after
// decompression. Protobuf decoding allocates a Go struct per repeated
// element, which can be 8-64 times larger than its wire encoding, so the
// elements are counted on the wire before anything is decoded.
type Limits struct {
	MaxSeries  int // time series per request
	MaxSamples int // samples per request; also bounds labels, symbols, histograms and exemplars
}

// errLimit is returned when a request exceeds Limits.
var errLimit = errors.New("request exceeds limits")

// Field numbers of the remote write protobuf messages.
const (
	v1TimeseriesField = 1 // prometheus.WriteRequest.timeseries
	v1LabelsField     = 1 // prometheus.TimeSeries.labels
	v2SymbolsField    = 4 // io.prometheus.write.v2.Request.symbols
	v2TimeseriesField = 5 // io.prometheus.write.v2.Request.timeseries
	v2LabelRefsField  = 1 // io.prometheus.write.v2.TimeSeries.labels_refs
	samplesField      = 2 // samples in both versions
)

// prescan walks the wire encoding of a remote write request and rejects it
// if it contains more elements than the limits allow.
func prescan(b []byte, v2 bool, lim Limits) error {
	var series, samples, other int
	check := func() error {
		switch {
		case series > lim.MaxSeries:
			return fmt.Errorf("%w: more than %d series", errLimit, lim.MaxSeries)
		case samples > lim.MaxSamples:
			return fmt.Errorf("%w: more than %d samples", errLimit, lim.MaxSamples)
		case other > lim.MaxSamples:
			return fmt.Errorf("%w: more than %d labels, symbols, histograms or exemplars", errLimit, lim.MaxSamples)
		}
		return nil
	}
	seriesField := protowire.Number(v1TimeseriesField)
	if v2 {
		seriesField = v2TimeseriesField
	}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return protowire.ParseError(m)
		}
		value := b[:m]
		b = b[m:]
		switch {
		case num == seriesField && typ == protowire.BytesType:
			series++
			v, _ := protowire.ConsumeBytes(value)
			s, o, err := countSeriesFields(v, v2)
			if err != nil {
				return err
			}
			samples += s
			other += o
		case v2 && num == v2SymbolsField:
			other++
		}
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// countSeriesFields counts the samples and other repeated elements of one
// encoded TimeSeries message.
func countSeriesFields(b []byte, v2 bool) (samples, other int, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, 0, protowire.ParseError(n)
		}
		b = b[n:]
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return 0, 0, protowire.ParseError(m)
		}
		switch {
		case num == samplesField:
			samples++
		case v2 && num == v2LabelRefsField && typ == protowire.BytesType:
			other += m // packed varints: the byte length bounds the count
		case num == v1LabelsField && !v2:
			other++
		default:
			other++ // histograms, exemplars, metadata, ...
		}
		b = b[m:]
	}
	return samples, other, nil
}
