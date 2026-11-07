package app

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

// The goroutines that the tests start must have stopped when they end. The
// goroutines of the TiKV client after Close have not been checked against a
// real cluster yet, so runs against one (XTSDB_TEST_TIKV_PD) skip the check.
func TestMain(m *testing.M) {
	if os.Getenv("XTSDB_TEST_TIKV_PD") != "" {
		os.Exit(m.Run())
	}
	goleak.VerifyTestMain(m)
}
