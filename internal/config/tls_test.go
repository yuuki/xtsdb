package config

import (
	"crypto/tls"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yuuki/xtsdb/internal/tlstest"
)

func TestServerTLS(t *testing.T) {
	if cfg, err := (ServerTLSConfig{}).Build(); cfg != nil || err != nil {
		t.Fatalf("TLS must be disabled without cert_file: %v %v", cfg, err)
	}
	f := tlstest.Generate(t, t.TempDir())
	cfg, err := ServerTLSConfig{CertFile: f.ServerCert, KeyFile: f.ServerKey, ClientCAFile: f.CA}.Build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert || cfg.ClientCAs == nil || cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("unexpected config %+v", cfg)
	}
	cfg, err = ServerTLSConfig{CertFile: f.ServerCert, KeyFile: f.ServerKey, ClientCAFile: f.CA,
		ClientAuthType: "VerifyClientCertIfGiven"}.Build()
	if err != nil || cfg.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("client_auth_type: %v %v", cfg, err)
	}
	if _, err := (ServerTLSConfig{CertFile: f.ServerCert, KeyFile: f.ClientKey}).Build(); err == nil {
		t.Fatal("a key that does not match the certificate must be refused")
	}
}

// A rotated certificate is served without a restart.
func TestServerTLSReloadsCertificate(t *testing.T) {
	dir := t.TempDir()
	f := tlstest.Generate(t, dir)
	r := &certReloader{certFile: f.ServerCert, keyFile: f.ServerKey}
	if err := r.load(); err != nil {
		t.Fatal(err)
	}
	first, _ := r.get(nil)

	g := tlstest.Generate(t, t.TempDir())
	for _, p := range [][2]string{{g.ServerCert, f.ServerCert}, {g.ServerKey, f.ServerKey}} {
		b, err := os.ReadFile(p[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p[1], b, 0o600); err != nil {
			t.Fatal(err)
		}
		future := time.Now().Add(time.Minute)
		if err := os.Chtimes(p[1], future, future); err != nil {
			t.Fatal(err)
		}
	}
	if same, _ := r.get(nil); same != first {
		t.Fatal("files must not be checked more often than certCheckInterval")
	}
	r.checked = time.Now().Add(-certCheckInterval)
	if second, _ := r.get(nil); second == first {
		t.Fatal("the rotated certificate was not loaded")
	}
	// A broken file keeps the last good certificate.
	if err := os.WriteFile(f.ServerCert, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.checked = time.Now().Add(-certCheckInterval)
	if c, _ := r.get(nil); c == nil {
		t.Fatal("a failed reload must keep the previous certificate")
	}
}

func TestValidateSecurity(t *testing.T) {
	cases := map[string]func(*Config){
		"server.tls.cert_file":           func(c *Config) { c.Server.TLS.CertFile = "x.pem" },
		"server.tls.client_ca_file":      func(c *Config) { c.Server.TLS.ClientCAFile = "ca.pem" },
		"ingester.graphite_tls":          func(c *Config) { c.Ingester.GraphiteTLS.KeyFile = "k.pem" },
		"client_auth_type":               func(c *Config) { c.Server.TLS.ClientAuthType = "NoClientCert" },
		"server.auth.write.basic_auth":   func(c *Config) { c.Server.Auth.Write.BasicAuth = []BasicAuthUser{{Username: "u"}} },
		"server.auth.read.bearer_tokens": func(c *Config) { c.Server.Auth.Read.BearerTokens = []string{""} },
		"unexpanded": func(c *Config) {
			c.Server.Auth.Write.BasicAuth = []BasicAuthUser{{Username: "u", Password: "${XTSDB_WRITE_PASSWORD}"}}
		},
	}
	for field, mutate := range cases {
		cfg := Default()
		mutate(cfg)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("%s: expected validation error, got %v", field, err)
		}
	}
	c := Credentials{BasicAuth: []BasicAuthUser{{Username: "u", Password: "p"}}, BearerTokens: []string{"t"}}
	if r := c.Redacted(); r.BasicAuth[0].Password == "p" || r.BearerTokens[0] == "t" || r.BasicAuth[0].Username != "u" {
		t.Fatalf("not redacted: %+v", r)
	}
}
