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
	yaml "go.yaml.in/yaml/v4"

	"github.com/yuuki/xtsdb/internal/app"
	"github.com/yuuki/xtsdb/internal/backendtest"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/model"
)

// TestImportCommand imports OpenMetrics files into each available
// persistent backend, then resumes with a state file. It needs
// XTSDB_TEST_CASSANDRA_HOSTS or XTSDB_TEST_TIKV_PD.
func TestImportCommand(t *testing.T) {
	backends := backendtest.Available()
	if len(backends) == 0 {
		t.Skip("XTSDB_TEST_CASSANDRA_HOSTS or XTSDB_TEST_TIKV_PD not set")
	}
	for _, b := range backends {
		t.Run(b, func(t *testing.T) { testImportCommand(t, b) })
	}
}

func testImportCommand(t *testing.T, backend string) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Storage.KeyPrefix = fmt.Sprintf("xtimport%d", rand.Uint32())
	cfg.Storage.Retention = config.Duration(30 * 24 * time.Hour)
	backendtest.Configure(t, cfg, backend)
	b, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfgFile := filepath.Join(dir, "xtsdb.yaml")
	if err := os.WriteFile(cfgFile, b, 0o600); err != nil {
		t.Fatal(err)
	}

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

	p, err := app.OpenPersistent(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	lset := labels.FromStrings("__name__", "backfilled", "part", "x")
	id := model.ID(lset)
	got, err := p.Cold.Read(context.Background(), []model.SeriesID{id}, start.UnixMilli(), time.Now().UnixMilli())
	if err != nil || len(got[id]) != 4320 {
		t.Fatalf("read %d samples, want 4320 (%v)", len(got[id]), err)
	}
	index := p.Index
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
