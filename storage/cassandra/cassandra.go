package cassandra

import (
	"encoding/binary"
	"time"
	"unsafe"

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
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 2,
		Min:        20 * time.Millisecond,
	}
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

// AddRows inserts rows into cassandra server.
func (c *Cassandra) AddRows(metricID string, datapoints []byte) error {
	if len(datapoints) < 8 {
		return nil
	}
	ts := binary.BigEndian.Uint64(datapoints[0:8])
	startTime := time.Unix(*(*int64)(unsafe.Pointer(&ts)), 0)
	err := c.session.Query(`
		INSERT INTO datapoint (metric_id, timestamp, values)
		VALUES (?, ?, ?)`, metricID, startTime, datapoints,
	).Exec()
	if err != nil {
		return xerrors.Errorf("Could not insert %q: %w", metricID, err)
	}
	return nil
}
