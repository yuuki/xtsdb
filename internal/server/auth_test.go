package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/yuuki/xtsdb/internal/config"
)

func TestAuthenticator(t *testing.T) {
	reg := prometheus.NewRegistry()
	failures := AuthFailures(reg)
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	if NewAuthenticator("read", config.Credentials{}, failures) != nil {
		t.Fatal("no credentials must mean no authenticator")
	}
	var open *Authenticator
	rec := httptest.NewRecorder()
	open.Wrap(ok).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("a nil authenticator must allow everything, got %d", rec.Code)
	}

	a := NewAuthenticator("write", config.Credentials{
		BasicAuth:    []config.BasicAuthUser{{Username: "prom", Password: "s3cret"}, {Username: "other", Password: "pw"}},
		BearerTokens: []string{"tok-1"},
	}, failures)
	h := a.Wrap(ok)
	cases := []struct {
		name string
		set  func(*http.Request)
		want int
	}{
		{"none", func(*http.Request) {}, http.StatusUnauthorized},
		{"basic", func(r *http.Request) { r.SetBasicAuth("prom", "s3cret") }, http.StatusNoContent},
		{"second user", func(r *http.Request) { r.SetBasicAuth("other", "pw") }, http.StatusNoContent},
		{"wrong password", func(r *http.Request) { r.SetBasicAuth("prom", "s3cre") }, http.StatusUnauthorized},
		{"password of another user", func(r *http.Request) { r.SetBasicAuth("prom", "pw") }, http.StatusUnauthorized},
		{"bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer tok-1") }, http.StatusNoContent},
		{"bearer lower case", func(r *http.Request) { r.Header.Set("Authorization", "bearer tok-1") }, http.StatusNoContent},
		{"wrong token", func(r *http.Request) { r.Header.Set("Authorization", "Bearer tok-2") }, http.StatusUnauthorized},
		{"empty token", func(r *http.Request) { r.Header.Set("Authorization", "Bearer ") }, http.StatusUnauthorized},
		{"other scheme", func(r *http.Request) { r.Header.Set("Authorization", "Digest tok-1") }, http.StatusUnauthorized},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", nil)
		c.set(req)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s: got %d, want %d", c.name, rec.Code, c.want)
		}
		if c.want == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: 401 without a challenge", c.name)
		}
	}
	mfs, err := reg.Gather()
	if err != nil || len(mfs) != 1 || len(mfs[0].GetMetric()) != 1 {
		t.Fatalf("gather: %v %v", mfs, err)
	}
	if n := mfs[0].GetMetric()[0].GetCounter().GetValue(); n != 6 {
		t.Fatalf("counted %v failures, want 6", n)
	}
}
