// Package version exposes build information.
package version

import (
	"runtime"
	"runtime/debug"
)

// Set at build time with -ldflags "-X github.com/yuuki/xtsdb/internal/version.Version=...".
var (
	Version   = "dev"
	Revision  = ""
	BuildDate = ""
)

func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	if Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
		Version = info.Main.Version
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if Revision == "" {
				Revision = s.Value
			}
		case "vcs.time":
			if BuildDate == "" {
				BuildDate = s.Value
			}
		}
	}
}

// Info returns build information in the shape of the Prometheus
// /api/v1/status/buildinfo response.
func Info() map[string]string {
	return map[string]string{
		"version":   Version,
		"revision":  Revision,
		"branch":    "",
		"buildUser": "",
		"buildDate": BuildDate,
		"goVersion": runtime.Version(),
	}
}
