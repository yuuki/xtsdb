package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/yuuki/xtsdb/internal/config"
)

// Authenticator checks the credentials of requests to one scope of the API.
// A nil *Authenticator allows every request.
type Authenticator struct {
	users    []userHash
	tokens   [][sha256.Size]byte
	failures prometheus.Counter
}

type userHash struct{ user, password [sha256.Size]byte }

// AuthFailures counts rejected requests by scope.
func AuthFailures(reg prometheus.Registerer) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "xtsdb_http_auth_failures_total",
		Help: "HTTP requests rejected for missing or invalid credentials, by scope.",
	}, []string{"scope"})
	reg.MustRegister(c)
	return c
}

// NewAuthenticator returns an Authenticator for credentials, or nil if none
// are configured. Secrets are kept only as SHA-256 hashes, which also makes
// comparisons independent of their length.
func NewAuthenticator(scope string, c config.Credentials, failures *prometheus.CounterVec) *Authenticator {
	if !c.Enabled() {
		return nil
	}
	a := &Authenticator{failures: failures.WithLabelValues(scope)}
	for _, u := range c.BasicAuth {
		a.users = append(a.users, userHash{sha256.Sum256([]byte(u.Username)), sha256.Sum256([]byte(u.Password))})
	}
	for _, t := range c.BearerTokens {
		a.tokens = append(a.tokens, sha256.Sum256([]byte(t)))
	}
	return a
}

// Wrap requires valid credentials for h. Requests without them get 401.
func (a *Authenticator) Wrap(h http.Handler) http.Handler {
	if a == nil {
		return h
	}
	challenge := `Bearer realm="xtsdb"`
	if len(a.users) > 0 {
		challenge = `Basic realm="xtsdb", charset="UTF-8"`
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.allowed(r) {
			h.ServeHTTP(w, r)
			return
		}
		a.failures.Inc()
		w.Header().Set("WWW-Authenticate", challenge)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func (a *Authenticator) allowed(r *http.Request) bool {
	if user, password, ok := r.BasicAuth(); ok {
		uh, ph := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(password))
		match := 0
		for _, u := range a.users {
			match |= subtle.ConstantTimeCompare(u.user[:], uh[:]) & subtle.ConstantTimeCompare(u.password[:], ph[:])
		}
		return match == 1
	}
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	th := sha256.Sum256([]byte(strings.TrimSpace(token)))
	match := 0
	for _, t := range a.tokens {
		match |= subtle.ConstantTimeCompare(t[:], th[:])
	}
	return match == 1
}
