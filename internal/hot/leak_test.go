package hot

import (
	"testing"

	"go.uber.org/goleak"
)

// The goroutines that the tests start must have stopped when they end.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
