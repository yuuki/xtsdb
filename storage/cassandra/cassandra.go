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
func (c *Cassandra) AddRows(mapRows map[string][]byte) error {
	batch := c.session.NewBatch(gocql.UnloggedBatch)
	for metricName, datapoints := range mapRows {
		if len(datapoints) < 8 {
			continue
		}
		ts := binary.BigEndian.Uint64(datapoints[0:8])
		startTime := time.Unix(*(*int64)(unsafe.Pointer(&ts)), 0)

		batch.Query(`
			INSERT INTO datapoint (metric_id, timestamp, values)
			VALUES (?, ?, ?)`, metricName, startTime, datapoints,
		)
	}
	if err := c.session.ExecuteBatch(batch); err != nil {
		return xerrors.Errorf("Got error ot batch: %w", err)
	}

	return nil
}
