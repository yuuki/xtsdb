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
	MaxSamples int // samples per request; also bounds labels, symbols, histogram buckets and exemplars
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
	v1HistogramsField = 4 // prometheus.TimeSeries.histograms
	v2HistogramsField = 3 // io.prometheus.write.v2.TimeSeries.histograms
)

// Field numbers of the repeated fields of a Histogram, the same in both
// versions.
const (
	histNegativeSpans  = 8
	histNegativeDeltas = 9
	histNegativeCounts = 10
	histPositiveSpans  = 11
	histPositiveDeltas = 12
	histPositiveCounts = 13
	histCustomValues   = 16
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
			return fmt.Errorf("%w: more than %d labels, symbols, histogram buckets or exemplars", errLimit, lim.MaxSamples)
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
		case typ == protowire.BytesType && ((v2 && num == v2HistogramsField) || (!v2 && num == v1HistogramsField)):
			// Buckets are packed: a few bytes on the wire become 8-byte
			// values, twice, so they count like other elements.
			v, _ := protowire.ConsumeBytes(b[:m])
			n, err := countHistogramElements(v)
			if err != nil {
				return 0, 0, err
			}
			other += n
		default:
			other++ // exemplars, metadata, ...
		}
		b = b[m:]
	}
	return samples, other, nil
}

// countHistogramElements returns an upper bound of the repeated elements
// (buckets, spans, custom bounds) of an encoded Histogram, plus one for the
// histogram itself.
func countHistogramElements(b []byte) (int, error) {
	n := 1
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return 0, protowire.ParseError(l)
		}
		b = b[l:]
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return 0, protowire.ParseError(m)
		}
		switch num {
		case histNegativeSpans, histPositiveSpans:
			n++
		case histNegativeDeltas, histPositiveDeltas, histNegativeCounts, histPositiveCounts, histCustomValues:
			if typ == protowire.BytesType {
				n += m // packed: at least one byte per element
			} else {
				n++
			}
		}
		b = b[m:]
	}
	return n, nil
}
