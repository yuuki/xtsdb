package cassandra

import (
	"log"
	"strconv"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v7"
	"github.com/gocql/gocql"
)

// Cassandra provides a cassandra client.
type Cassandra struct {
	session *gocql.Session
}

// New creates a Cassandra client.
func New() (*Cassandra, error) {
	// TODO: Support multi node
	cluster := gocql.NewCluster("127.0.0.1:9042")
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
func (c *Cassandra) AddRows(metricName string, xmsgs []goredis.XMessage) error {
	for _, xmsg := range xmsgs {
		ts := strings.TrimSuffix(xmsg.ID, "-0")
		t, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			log.Printf("Could not parse %s: %s\n", ts, err)
			continue
		}

		timestamp := time.Unix(t, 0)

		// TODO: Insert block of datapoints to reduce I/O/
		for _, v := range xmsg.Values {
			val, err := strconv.ParseFloat(v.(string), 64)
			if err != nil {
				log.Printf("Could not parse %v: %s\n", v, err)
				continue
			}
			err = c.session.Query(`
				INSERT INTO datapoint (metric_id, timestamp, value)
				VALUES (?, ?, ?) `, metricName, timestamp, val,
			).Exec()
			if err != nil {
				log.Printf("Could not insert datapoint (%v, %v, %v): %s\n",
					metricName, timestamp, val, err)
				continue
			}
		}
	}

	return nil
}
