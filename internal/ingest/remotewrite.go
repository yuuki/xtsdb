package ingest

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

	"github.com/yuuki/xtsdb/internal/model"
)

// Remote write protobuf message names, as used in the Content-Type header.
const (
	protoV1 = "prometheus.WriteRequest"
	protoV2 = "io.prometheus.write.v2.Request"
)

// Response headers defined by Remote Write 2.0.
const (
	headerSamplesWritten    = "X-Prometheus-Remote-Write-Samples-Written"
	headerHistogramsWritten = "X-Prometheus-Remote-Write-Histograms-Written"
	headerExemplarsWritten  = "X-Prometheus-Remote-Write-Exemplars-Written"
)

// RemoteWriteOptions bounds the resources a remote write request may use.
type RemoteWriteOptions struct {
	MaxBodySize         int64 // compressed body
	MaxDecompressedSize int64
	MaxInflight         int // concurrent requests; more get 503 and are retried
	Limits              Limits
}

// RemoteWriteHandler serves the Prometheus Remote Write 1.0 and 2.0
// protocols. Native histograms and exemplars are not supported: requests
// carrying histograms store their float samples and answer 400.
func RemoteWriteHandler(w *Writer, opts RemoteWriteOptions, logger *slog.Logger) http.Handler {
	inflight := make(chan struct{}, opts.MaxInflight)
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		select {
		case inflight <- struct{}{}:
			defer func() { <-inflight }()
		default:
			w.discard("throttled", 0)
			http.Error(rw, "too many concurrent remote write requests", http.StatusServiceUnavailable)
			return
		}
		proto, err := remoteWriteProto(r.Header.Get("Content-Type"))
		if err != nil {
			http.Error(rw, err.Error(), http.StatusUnsupportedMediaType)
			return
		}
		if enc := r.Header.Get("Content-Encoding"); enc != "" && !strings.EqualFold(enc, "snappy") {
			http.Error(rw, "unsupported Content-Encoding "+enc, http.StatusUnsupportedMediaType)
			return
		}
		compressed, err := io.ReadAll(http.MaxBytesReader(rw, r.Body, opts.MaxBodySize))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(rw, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		n, err := snappy.DecodedLen(compressed)
		if err != nil {
			http.Error(rw, "snappy: "+err.Error(), http.StatusBadRequest)
			return
		}
		if int64(n) > opts.MaxDecompressedSize {
			http.Error(rw, fmt.Sprintf("decompressed request of %d bytes exceeds the limit of %d", n, opts.MaxDecompressedSize),
				http.StatusRequestEntityTooLarge)
			return
		}
		// Strict Snappy: the S2 extensions accepted by snappy.Decode allow
		// far higher compression ratios.
		body, err := snappy.DecodeStrict(nil, compressed)
		if err != nil {
			http.Error(rw, "snappy: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := prescan(body, proto == protoV2, opts.Limits); err != nil {
			code := http.StatusBadRequest
			if errors.Is(err, errLimit) {
				code = http.StatusRequestEntityTooLarge
			}
			http.Error(rw, err.Error(), code)
			return
		}

		var (
			batch      []Series
			histograms int
		)
		if proto == protoV2 {
			batch, histograms, err = decodeV2(body)
		} else {
			batch, histograms, err = decodeV1(body)
		}
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		res, err := w.Write(r.Context(), batch)
		if histograms > 0 {
			w.discard("unsupported", histograms)
		}
		if proto == protoV2 {
			rw.Header().Set(headerSamplesWritten, strconv.Itoa(res.Accepted))
			rw.Header().Set(headerHistogramsWritten, "0")
			rw.Header().Set(headerExemplarsWritten, "0")
		}
		switch {
		case err != nil:
			logger.Warn("remote write failed", "err", err)
			http.Error(rw, "storage unavailable, retry later", http.StatusServiceUnavailable)
		case res.Rejected() > 0 || histograms > 0:
			// Partial success. 4xx tells the sender not to retry.
			msg := describeRejections(res)
			if histograms > 0 {
				msg += fmt.Sprintf("; %d native histogram samples dropped: native histograms are not supported", histograms)
			}
			http.Error(rw, msg, http.StatusBadRequest)
		default:
			rw.WriteHeader(http.StatusNoContent)
		}
	})
}

func remoteWriteProto(contentType string) (string, error) {
	if contentType == "" {
		return protoV1, nil
	}
	mt, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", fmt.Errorf("invalid Content-Type: %w", err)
	}
	if mt != "application/x-protobuf" {
		return "", fmt.Errorf("unsupported Content-Type %q", contentType)
	}
	switch p := params["proto"]; p {
	case "", protoV1:
		return protoV1, nil
	case protoV2:
		return protoV2, nil
	default:
		return "", fmt.Errorf("unsupported remote write proto %q", p)
	}
}

func decodeV1(body []byte) ([]Series, int, error) {
	var req prompb.WriteRequest
	if err := req.Unmarshal(body); err != nil {
		return nil, 0, fmt.Errorf("decode remote write 1.0 request: %w", err)
	}
	batch := make([]Series, 0, len(req.Timeseries))
	dropped := 0
	b := labels.NewScratchBuilder(16)
	for _, ts := range req.Timeseries {
		dropped += len(ts.Histograms)
		if len(ts.Samples) == 0 {
			continue
		}
		b.Reset()
		for _, l := range ts.Labels {
			b.Add(l.Name, l.Value)
		}
		samples := make([]model.Sample, len(ts.Samples))
		for i, s := range ts.Samples {
			samples[i] = model.Sample{T: s.Timestamp, V: s.Value}
		}
		batch = append(batch, Series{Labels: b.Labels(), Samples: samples})
	}
	return batch, dropped, nil
}

func decodeV2(body []byte) ([]Series, int, error) {
	var req writev2.Request
	if err := req.Unmarshal(body); err != nil {
		return nil, 0, fmt.Errorf("decode remote write 2.0 request: %w", err)
	}
	batch := make([]Series, 0, len(req.Timeseries))
	dropped := 0
	b := labels.NewScratchBuilder(16)
	for _, ts := range req.Timeseries {
		dropped += len(ts.Histograms)
		if len(ts.Samples) == 0 {
			continue
		}
		lset, err := ts.ToLabels(&b, req.Symbols)
		if err != nil {
			return nil, 0, fmt.Errorf("decode labels: %w", err)
		}
		samples := make([]model.Sample, len(ts.Samples))
		for i, s := range ts.Samples {
			samples[i] = model.Sample{T: s.Timestamp, V: s.Value}
		}
		batch = append(batch, Series{Labels: lset, Samples: samples})
	}
	return batch, dropped, nil
}

func describeRejections(res Result) string {
	var parts []string
	if res.TooOld > 0 {
		parts = append(parts, fmt.Sprintf("%d samples too old (their chunk window is closed)", res.TooOld))
	}
	if res.TooNew > 0 {
		parts = append(parts, fmt.Sprintf("%d samples too far in the future", res.TooNew))
	}
	if res.Invalid > 0 {
		parts = append(parts, fmt.Sprintf("%d samples of invalid series (first: %v)", res.Invalid, res.FirstInvalid))
	}
	if res.Collision > 0 {
		parts = append(parts, fmt.Sprintf("%d samples of series whose ID collides with another series", res.Collision))
	}
	return fmt.Sprintf("accepted %d samples, rejected %s", res.Accepted, strings.Join(parts, ", "))
}
