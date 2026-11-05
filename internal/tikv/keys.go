package tikv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
)

// The storage format. Every key starts with storage.key_prefix, a ':' (which
// the prefix cannot contain) and a tag naming the kind of entry:
//
//	m <name>                                                   layout parameter (meta)
//	c <series> <start> <fragment> <total> <digest> <part> <seg> chunk segment
//	s <series>                                                 labels of a series
//	p <day> <name> <value> <series>                            posting
//	v <day> <name> <value>                                     label value
//	n <day> <name>                                             label name
//
// Integers are fixed-width big-endian, signed ones with the sign bit
// flipped, so that keys sort like the numbers: the chunk segments of a
// series are ordered by window start, then by version in the order of
// precedence (fragment, total, digest as a signed value; see
// storage.ColdChunk), then by part and segment. Strings are prefixed with
// their length (uvarint), which makes every key unambiguous whatever bytes
// the string holds; lookups never rely on the order of strings.
const (
	tagMeta    = 'm'
	tagChunk   = 'c'
	tagSeries  = 's'
	tagPosting = 'p'
	tagValue   = 'v'
	tagName    = 'n'
)

// formatVersion is recorded under the meta key "format" and in the first
// byte of every value.
const formatVersion = 1

// segmentSize is the most data bytes held in one chunk segment. A part
// larger than that is split over several keys, so that every value stays
// small: TiKV sends at most 10 MiB in one response by default
// (server.max-grpc-send-msg-len), and reads take as many keys at a time as
// fit in scanBudget. It is part of the storage format.
const segmentSize = 64 << 10

// chunkHeaderLen is the length of the header of a chunk segment value:
// format, encoding, segment count, max_t and sample count.
const chunkHeaderLen = 1 + 1 + 4 + 8 + 4

// chunkKeyLen is the length of a chunk segment key after the table prefix.
const chunkKeyLen = 8*6 + 4

var errMalformed = errors.New("malformed entry")

// keys builds keys under a prefix.
type keys struct {
	prefix []byte // storage.key_prefix + ':'
}

func newKeys(prefix string) keys { return keys{prefix: []byte(prefix + ":")} }

// table returns the prefix of all keys with a tag.
func (k keys) table(tag byte) []byte {
	return append(append(make([]byte, 0, len(k.prefix)+64), k.prefix...), tag)
}

func appendUint64(b []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(b, v) }

func appendInt64(b []byte, v int64) []byte { return appendUint64(b, uint64(v)^1<<63) }

func appendDay(b []byte, day int32) []byte {
	return binary.BigEndian.AppendUint32(b, uint32(day)^1<<31)
}

func appendString(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

func getInt64(b []byte) int64 { return int64(binary.BigEndian.Uint64(b) ^ 1<<63) }

// getString decodes a length-prefixed string and returns the rest.
func getString(b []byte) (string, []byte, error) {
	n, w := binary.Uvarint(b)
	if w <= 0 || uint64(len(b)-w) < n {
		return "", nil, errMalformed
	}
	return string(b[w : w+int(n)]), b[w+int(n):], nil
}

func (k keys) meta(name string) []byte { return append(k.table(tagMeta), name...) }

// chunkSeries is the prefix of the chunk segments of a series.
func (k keys) chunkSeries(id model.SeriesID) []byte {
	return appendUint64(k.table(tagChunk), uint64(id))
}

// chunkFrom is the first key of the chunks of a series starting at start or
// later.
func (k keys) chunkFrom(id model.SeriesID, start int64) []byte {
	return appendInt64(k.chunkSeries(id), start)
}

// chunkUntil is the end (exclusive) of the chunks of a series starting at
// start or earlier.
func (k keys) chunkUntil(id model.SeriesID, start int64) []byte {
	if start == math.MaxInt64 {
		return prefixEnd(k.chunkSeries(id))
	}
	return k.chunkFrom(id, start+1)
}

// segmentKey identifies a segment of a part of a chunk version.
type segmentKey struct {
	start, fragment, total, digest, part int64
	seg                                  uint32
}

func (s segmentKey) sameVersion(o segmentKey) bool {
	return s.start == o.start && s.fragment == o.fragment && s.total == o.total && s.digest == o.digest
}

func (k keys) chunk(id model.SeriesID, s segmentKey) []byte {
	b := k.chunkSeries(id)
	for _, v := range []int64{s.start, s.fragment, s.total, s.digest, s.part} {
		b = appendInt64(b, v)
	}
	return binary.BigEndian.AppendUint32(b, s.seg)
}

// parseChunk decodes the key of a chunk segment of a series, given the
// length of the prefix of the series' chunks.
func parseChunk(key []byte, seriesPrefixLen int) (segmentKey, error) {
	b := key[min(seriesPrefixLen, len(key)):]
	if len(b) != chunkKeyLen-8 {
		return segmentKey{}, errMalformed
	}
	var s segmentKey
	for i, p := range []*int64{&s.start, &s.fragment, &s.total, &s.digest, &s.part} {
		*p = getInt64(b[8*i:])
	}
	s.seg = binary.BigEndian.Uint32(b[40:])
	return s, nil
}

// segmentHeader is the header of a chunk segment value.
type segmentHeader struct {
	encoding chunk.Encoding
	segments uint32 // segments of the part
	maxT     int64
	count    uint32 // samples in the part
}

func appendSegment(b []byte, h segmentHeader, data []byte) []byte {
	b = append(b, formatVersion, byte(h.encoding))
	b = binary.BigEndian.AppendUint32(b, h.segments)
	b = binary.BigEndian.AppendUint64(b, uint64(h.maxT))
	b = binary.BigEndian.AppendUint32(b, h.count)
	return append(b, data...)
}

func parseSegment(v []byte) (segmentHeader, []byte, error) {
	if len(v) < chunkHeaderLen || v[0] != formatVersion {
		return segmentHeader{}, nil, errMalformed
	}
	h := segmentHeader{
		encoding: chunk.Encoding(v[1]),
		segments: binary.BigEndian.Uint32(v[2:]),
		maxT:     int64(binary.BigEndian.Uint64(v[6:])),
		count:    binary.BigEndian.Uint32(v[14:]),
	}
	if h.segments == 0 {
		return segmentHeader{}, nil, errMalformed
	}
	return h, v[chunkHeaderLen:], nil
}

func (k keys) series(id model.SeriesID) []byte { return appendUint64(k.table(tagSeries), uint64(id)) }

// encodeLabels encodes a label set deterministically, so that equal label
// sets have equal encodings: the series row is claimed by compare-and-swap.
func encodeLabels(lset labels.Labels) []byte {
	b := make([]byte, 1, 1+lset.Len()*16)
	b[0] = formatVersion
	lset.Range(func(l labels.Label) {
		b = appendString(appendString(b, l.Name), l.Value)
	})
	return b
}

func decodeLabels(v []byte) (labels.Labels, error) {
	if len(v) == 0 || v[0] != formatVersion {
		return labels.EmptyLabels(), errMalformed
	}
	b := labels.NewScratchBuilder(8)
	for rest := v[1:]; len(rest) > 0; {
		name, r, err := getString(rest)
		if err != nil {
			return labels.EmptyLabels(), err
		}
		value, r, err := getString(r)
		if err != nil {
			return labels.EmptyLabels(), err
		}
		b.Add(name, value)
		rest = r
	}
	b.Sort()
	return b.Labels(), nil
}

// labelPair is the prefix of the postings of a label pair on a day.
func (k keys) labelPair(day int32, name, value string) []byte {
	return appendString(appendString(appendDay(k.table(tagPosting), day), name), value)
}

func (k keys) posting(day int32, name, value string, id model.SeriesID) []byte {
	return appendUint64(k.labelPair(day, name, value), uint64(id))
}

// labelValues is the prefix of the values of a label name on a day.
func (k keys) labelValues(day int32, name string) []byte {
	return appendString(appendDay(k.table(tagValue), day), name)
}

func (k keys) labelValue(day int32, name, value string) []byte {
	return appendString(k.labelValues(day, name), value)
}

// labelNames is the prefix of the label names of a day.
func (k keys) labelNames(day int32) []byte { return appendDay(k.table(tagName), day) }

func (k keys) labelName(day int32, name string) []byte { return appendString(k.labelNames(day), name) }

// presence is the value of index entries whose key holds all the
// information.
var presence = []byte{formatVersion}

// prefixEnd returns the smallest key greater than every key with the given
// prefix.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	panic(fmt.Sprintf("no key follows the prefix %q", prefix))
}
