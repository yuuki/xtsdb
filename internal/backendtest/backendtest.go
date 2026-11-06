// Package backendtest selects the persistent backends that integration
// tests run against.
package backendtest

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/yuuki/xtsdb/internal/cassandra"
	"github.com/yuuki/xtsdb/internal/config"
)

// Available returns the backends whose servers are given in the
// environment: cassandra with XTSDB_TEST_CASSANDRA_HOSTS (comma separated)
// and tikv with XTSDB_TEST_TIKV_PD (comma separated PD addresses, and
// optionally XTSDB_TEST_TIKV_API_VERSION).
func Available() []string {
	var out []string
	if os.Getenv("XTSDB_TEST_CASSANDRA_HOSTS") != "" {
		out = append(out, config.BackendCassandra)
	}
	if os.Getenv("XTSDB_TEST_TIKV_PD") != "" && tikvSupported {
		out = append(out, config.BackendTiKV)
	}
	return out
}

// Configure points cfg at a fresh namespace of a backend: a new Cassandra
// keyspace (created on first use with cassandra.auto_migrate), or the key
// prefix cfg.Storage.KeyPrefix in TiKV, which must be unique to the test.
// The data is deleted when the test ends.
func Configure(t *testing.T, cfg *config.Config, backend string) {
	t.Helper()
	cfg.Storage.Backend = backend
	switch backend {
	case config.BackendCassandra:
		cfg.Cassandra.Hosts = strings.Split(os.Getenv("XTSDB_TEST_CASSANDRA_HOSTS"), ",")
		cfg.Cassandra.Keyspace = fmt.Sprintf("xttest_%d", rand.Uint32())
		cfg.Cassandra.Consistency = "ONE"
		cfg.Cassandra.AutoMigrate = true
		ccfg := cfg.Cassandra
		t.Cleanup(func() {
			s, err := cassandra.NewSession(ccfg, nil, false)
			if err != nil {
				t.Logf("cleanup: %v", err)
				return
			}
			defer s.Close()
			_ = s.Query("DROP KEYSPACE IF EXISTS " + ccfg.Keyspace).Exec()
		})
	case config.BackendTiKV:
		cfg.TiKV.PDEndpoints = strings.Split(os.Getenv("XTSDB_TEST_TIKV_PD"), ",")
		if v := os.Getenv("XTSDB_TEST_TIKV_API_VERSION"); v != "" {
			cfg.TiKV.APIVersion = v
		}
		tcfg, prefix := cfg.TiKV, cfg.Storage.KeyPrefix
		t.Cleanup(func() { deleteTiKV(t, tcfg, prefix) })
	default:
		t.Fatalf("unknown backend %q", backend)
	}
}
