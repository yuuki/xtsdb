package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/cassandra"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/model"
)

// TestImportCommand imports OpenMetrics files into Cassandra, then resumes
// with a state file. It needs XTSDB_TEST_CASSANDRA_HOSTS.
func TestImportCommand(t *testing.T) {
	hosts := os.Getenv("XTSDB_TEST_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Skip("XTSDB_TEST_CASSANDRA_HOSTS not set")
	}
	dir := t.TempDir()
	keyspace := fmt.Sprintf("xtimport_%d", rand.Uint32())
	cfgFile := filepath.Join(dir, "xtsdb.yaml")
	err := os.WriteFile(cfgFile, []byte(fmt.Sprintf(`
cassandra:
  hosts: [%s]
  keyspace: %s
  consistency: ONE
  auto_migrate: true
storage:
  retention: 30d
`, hosts, keyspace)), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(cfgFile, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s, err := cassandra.NewSession(cfg.Cassandra, nil, false); err == nil {
			_ = s.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
			s.Close()
		}
	})

	start := time.Now().Add(-72 * time.Hour).Truncate(time.Hour)
	files := make([]string, 2)
	for i := range files {
		var b strings.Builder
		for m := range 3 * 24 * 60 / 2 { // the first or second half of three days
			ts := start.Add(time.Duration(i*3*24*60/2+m) * time.Minute)
			fmt.Fprintf(&b, "backfilled{part=\"x\"} %d %d\n", m, ts.Unix())
		}
		b.WriteString("# EOF\n")
		files[i] = filepath.Join(dir, fmt.Sprintf("part%d.om", i))
		if err := os.WriteFile(files[i], []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	state := filepath.Join(dir, "state")
	run1 := func(args ...string) string {
		t.Helper()
		var out, errOut bytes.Buffer
		args = append([]string{"import", "-config.file=" + cfgFile, "-format=openmetrics", "-state-file=" + state, "-log.level=warn"}, args...)
		if code := run(args, &out, &errOut); code != 0 {
			t.Fatalf("import exited %d: %s", code, errOut.String())
		}
		return out.String()
	}
	if out := run1(files[0]); !strings.Contains(out, "imported 2160 samples (0 native histograms) of 1 series") {
		t.Fatalf("unexpected output %q", out)
	}
	// The first file is skipped, the second one is imported.
	if out := run1(files[0], files[1]); !strings.Contains(out, "imported 2160 samples") {
		t.Fatalf("unexpected output %q", out)
	}
	if out := run1(files[0], files[1]); !strings.Contains(out, "imported 0 samples") {
		t.Fatalf("everything must be skipped, got %q", out)
	}

	s, err := cassandra.NewSession(cfg.Cassandra, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lset := labels.FromStrings("__name__", "backfilled", "part", "x")
	id := model.ID(lset)
	cold := cassandra.NewChunkStore(s, cfg.Storage.Retention.D(), cfg.Storage.ChunkRange.D().Milliseconds(), 8, nil)
	got, err := cold.Read(context.Background(), []model.SeriesID{id}, start.UnixMilli(), time.Now().UnixMilli())
	if err != nil || len(got[id]) != 4320 {
		t.Fatalf("read %d samples, want 4320 (%v)", len(got[id]), err)
	}
	index := cassandra.NewIndex(s, cfg.Storage.Retention.D(), 8, cfg.Storage.IndexBuckets)
	for _, day := range model.Days(start.UnixMilli(), start.Add(72*time.Hour-time.Minute).UnixMilli()) {
		if ids, err := index.Postings(context.Background(), day, "part", "x"); err != nil || len(ids) != 1 {
			t.Fatalf("day %d not indexed: %v %v", day, ids, err)
		}
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"import", "-config.file=" + cfgFile}, &out, &errOut); code != 2 {
		t.Fatalf("import without paths: exit %d", code)
	}
}
