package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommands(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"version"}, &out, &errOut); code != 0 || !strings.HasPrefix(out.String(), "xtsdb ") {
		t.Fatalf("version: %d %q", code, out.String())
	}
	if code := run(nil, &out, &errOut); code != 2 {
		t.Fatalf("no command: exit %d", code)
	}
	if code := run([]string{"bogus"}, &out, &errOut); code != 2 {
		t.Fatalf("unknown command: exit %d", code)
	}
	// A stray argument would otherwise silently stop flag parsing.
	if code := run([]string{"all", "extra", "-config.file=x.yaml"}, &out, &errOut); code != 2 {
		t.Fatalf("positional argument: exit %d", code)
	}
	for _, args := range [][]string{
		{"quarantine"},
		{"quarantine", "bogus"},
		{"quarantine", "list", "extra"},
		{"quarantine", "delete"},                       // neither keys nor -all
		{"quarantine", "delete", "-all", "xt:{0}:q:x"}, // both
	} {
		if code := run(args, &out, &errOut); code != 2 {
			t.Fatalf("%v: exit %d", args, code)
		}
	}
}

func TestConfigCommandRedactsSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte("redis:\n  password: hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"config", "-config.file=" + path}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if strings.Contains(out.String(), "hunter2") || !strings.Contains(out.String(), "<redacted>") {
		t.Fatalf("password not redacted:\n%s", out.String())
	}
}
