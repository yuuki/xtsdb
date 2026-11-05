// Package tlstest generates certificates for tests.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Files are PEM files of a CA and of a server and a client certificate
// signed by it. The server certificate is valid for 127.0.0.1, ::1 and
// localhost.
type Files struct {
	CA                    string
	ServerCert, ServerKey string
	ClientCert, ClientKey string
	pool                  *x509.CertPool
}

// Generate writes a CA, a server and a client certificate to dir.
func Generate(tb testing.TB, dir string) Files {
	tb.Helper()
	caKey, caCert, caDER := newCert(tb, nil, nil, &x509.Certificate{
		Subject:               pkix.Name{CommonName: "xtsdb test CA"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	})
	f := Files{
		CA:         filepath.Join(dir, "ca.pem"),
		ServerCert: filepath.Join(dir, "server.pem"),
		ServerKey:  filepath.Join(dir, "server-key.pem"),
		ClientCert: filepath.Join(dir, "client.pem"),
		ClientKey:  filepath.Join(dir, "client-key.pem"),
		pool:       x509.NewCertPool(),
	}
	f.pool.AddCert(caCert)
	write(tb, f.CA, "CERTIFICATE", caDER)
	for _, c := range []struct {
		cert, key string
		tmpl      *x509.Certificate
	}{
		{f.ServerCert, f.ServerKey, &x509.Certificate{
			Subject:     pkix.Name{CommonName: "localhost"},
			DNSNames:    []string{"localhost"},
			IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}},
		{f.ClientCert, f.ClientKey, &x509.Certificate{
			Subject:     pkix.Name{CommonName: "client"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}},
	} {
		key, _, der := newCert(tb, caCert, caKey, c.tmpl)
		write(tb, c.cert, "CERTIFICATE", der)
		kb, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			tb.Fatal(err)
		}
		write(tb, c.key, "EC PRIVATE KEY", kb)
	}
	return f
}

// ClientConfig returns a TLS client configuration that trusts the CA and,
// with withCert, presents the client certificate.
func (f Files) ClientConfig(tb testing.TB, withCert bool) *tls.Config {
	tb.Helper()
	cfg := &tls.Config{RootCAs: f.pool, MinVersion: tls.VersionTLS12}
	if withCert {
		cert, err := tls.LoadX509KeyPair(f.ClientCert, f.ClientKey)
		if err != nil {
			tb.Fatal(err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

var serial int64

func newCert(tb testing.TB, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, tmpl *x509.Certificate) (*ecdsa.PrivateKey, *x509.Certificate, []byte) {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	serial++
	tmpl.SerialNumber = big.NewInt(serial)
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(24 * time.Hour)
	if parent == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		tb.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		tb.Fatal(err)
	}
	return key, cert, der
}

func write(tb testing.TB, path, typ string, der []byte) {
	tb.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		tb.Fatal(err)
	}
}
