package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"
)

// Build returns a *tls.Config, or nil when TLS is disabled.
func (t TLSConfig) Build() (*tls.Config, error) {
	if !t.Enabled {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // explicit opt-in for test setups
	}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %s contains no certificates", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" || t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// ServerTLSConfig configures TLS for a listener. TLS is enabled when
// cert_file is set.
type ServerTLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// ClientCAFile enables mutual TLS: client certificates are verified
	// against these CAs.
	ClientCAFile string `yaml:"client_ca_file"`
	// ClientAuthType is RequireAndVerifyClientCert (the default with
	// client_ca_file) or VerifyClientCertIfGiven.
	ClientAuthType string `yaml:"client_auth_type"`
}

// Enabled reports whether TLS is configured.
func (t ServerTLSConfig) Enabled() bool { return t.CertFile != "" }

func (t ServerTLSConfig) validate(field string) []error {
	var errs []error
	if (t.CertFile == "") != (t.KeyFile == "") {
		errs = append(errs, fmt.Errorf("%s.cert_file and %s.key_file must be set together", field, field))
	}
	if t.ClientCAFile != "" && t.CertFile == "" {
		errs = append(errs, fmt.Errorf("%s.client_ca_file requires cert_file and key_file", field))
	}
	switch t.ClientAuthType {
	case "", "RequireAndVerifyClientCert", "VerifyClientCertIfGiven":
	default:
		errs = append(errs, fmt.Errorf("%s.client_auth_type must be RequireAndVerifyClientCert or VerifyClientCertIfGiven", field))
	}
	if t.ClientAuthType != "" && t.ClientCAFile == "" {
		errs = append(errs, fmt.Errorf("%s.client_auth_type requires client_ca_file", field))
	}
	return errs
}

// Build returns a *tls.Config for a server, or nil when TLS is disabled.
// The certificate and key are read again when their files change, so that
// rotated certificates are picked up without a restart.
func (t ServerTLSConfig) Build() (*tls.Config, error) {
	if !t.Enabled() {
		return nil, nil
	}
	r := &certReloader{certFile: t.CertFile, keyFile: t.KeyFile}
	if err := r.load(); err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.get,
	}
	if t.ClientCAFile != "" {
		pem, err := os.ReadFile(t.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client_ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client_ca_file %s contains no certificates", t.ClientCAFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		if t.ClientAuthType == "VerifyClientCertIfGiven" {
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return cfg, nil
}

// certReloader serves a certificate and reloads it when its files change.
// Files are checked at most once per certCheckInterval; if a reload fails,
// the previous certificate is kept.
type certReloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	mtime   [2]time.Time
	checked time.Time
}

const certCheckInterval = 10 * time.Second

func (r *certReloader) modTimes() ([2]time.Time, error) {
	var out [2]time.Time
	for i, f := range []string{r.certFile, r.keyFile} {
		st, err := os.Stat(f)
		if err != nil {
			return out, err
		}
		out[i] = st.ModTime()
	}
	return out, nil
}

func (r *certReloader) load() error {
	mtime, err := r.modTimes()
	if err != nil {
		return fmt.Errorf("load server certificate: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("load server certificate: %w", err)
	}
	r.cert, r.mtime, r.checked = &cert, mtime, time.Now()
	return nil
}

func (r *certReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.checked) >= certCheckInterval {
		r.checked = time.Now()
		if mtime, err := r.modTimes(); err == nil && mtime != r.mtime {
			_ = r.load() // keep serving the previous certificate on failure
		}
	}
	return r.cert, nil
}
