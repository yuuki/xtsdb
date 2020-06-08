package cassandra

import (
	"encoding/binary"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v7"
	"github.com/gocql/gocql"
	"golang.org/x/xerrors"

	"github.com/yuuki/xtsdb/config"
)

// Cassandra provides a cassandra client.
type Cassandra struct {
	session *gocql.Session
}

// New creates a Cassandra client.
func New() (*Cassandra, error) {
	cluster := gocql.NewCluster(config.Config.CassandraAddrs...)
	cluster.Keyspace = "xtsdb"
	cluster.Consistency = gocql.Any
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &Cassandra{session: session}, nil
}

// Close closes the session.
func (c *Cassandra) Close() {
	c.session.Close()
}

type datapoint struct {
	timestamp int64
	value     float64
}

// AddRows inserts rows into cassandra server.
func (c *Cassandra) AddRows(metricName string, xmsgs []goredis.XMessage) error {
	blob := make([]byte, len(xmsgs)*2*8) // 2 -> (timestamp, value) 8 -> bytes of int64 or float64

	var startTime time.Time

	for i, xmsg := range xmsgs {
		ts := strings.TrimSuffix(xmsg.ID, "-0")
		t, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			log.Printf("Could not parse %s: %s\n", ts, err)
			continue
		}
		if startTime.IsZero() {
			startTime = time.Unix(t, 0)
		}

		binary.BigEndian.PutUint64(blob[i*2*8:], uint64(t))

		v := xmsg.Values[""]
		val, err := strconv.ParseFloat(v.(string), 64)
		if err != nil {
			log.Printf("Could not parse %v: %s\n", v, err)
			continue
		}

		binary.BigEndian.PutUint64(blob[i*2*8+8:], math.Float64bits(val))
	}

	err := c.session.Query(`
		INSERT INTO datapoint (metric_id, timestamp, values)
		VALUES (?, ?, ?)`, metricName, startTime, blob,
	).Exec()
	if err != nil {
		return xerrors.Errorf("Could not insert datapoint (%v, %v): %w\n",
			metricName, startTime, err)
	}

	return nil
}
