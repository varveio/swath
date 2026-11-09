// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// tokenServer is a local OAuth2 token endpoint: each refresh issues
// tok-N, valid for expires seconds (oauth2 treats a token as expired ten
// seconds early, so 1 forces a refresh on every use).
type tokenServer struct {
	*httptest.Server
	refreshes atomic.Int64
	expires   int
	delay     time.Duration // before answering
	status    int           // non-zero: answer with this status
}

func newTokenServer(t *testing.T, expires int) *tokenServer {
	ts := &tokenServer{expires: expires}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := ts.refreshes.Add(1)
		if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "rt" {
			http.Error(w, "bad grant", 400)
			return
		}
		select {
		case <-time.After(ts.delay):
		case <-r.Context().Done():
			return
		}
		if ts.status != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(ts.status)
			fmt.Fprint(w, `{"error":"invalid_grant","error_description":"Token has been expired or revoked."}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"tok-%d","token_type":"Bearer","expires_in":%d}`, n, ts.expires)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// listServer answers objects.list with one object and records each
// request's Authorization header.
type listServer struct {
	*httptest.Server
	mu    sync.Mutex
	auths []string
}

func newListServer(t *testing.T) *listServer {
	ls := &listServer{}
	ls.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ls.mu.Lock()
		ls.auths = append(ls.auths, r.Header.Get("Authorization"))
		ls.mu.Unlock()
		if r.Header.Get("Authorization") == "" {
			http.Error(w, `{"error":{"code":401,"message":"no credentials","errors":[{"reason":"required"}]}}`, 401)
			return
		}
		fmt.Fprint(w, `{"kind":"storage#objects","items":[{"name":"a","size":"1"}]}`)
	}))
	t.Cleanup(ls.Close)
	return ls
}

func (ls *listServer) seen() []string {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return append([]string(nil), ls.auths...)
}

// adc points Application Default Credentials at an authorized-user file
// (gcloud auth application-default login writes this shape) whose token
// endpoint is tokenURL; an empty HOME keeps the developer's own gcloud
// file out of the test.
func adc(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("APPDATA", dir)
	f := filepath.Join(dir, "adc.json")
	if err := os.WriteFile(f, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", f)
	return f
}

func authorizedUser(tokenURL string) string {
	return fmt.Sprintf(`{"type":"authorized_user","client_id":"cid","client_secret":"cs","refresh_token":"rt","token_uri":%q}`, tokenURL)
}

// TestAuthorizedUserADC: ordinary gcloud credentials (authorized-user ADC)
// get a token at the first request and a fresh one when it expires,
// although the context New ran under has ended; the listing carries the
// token (the old client gave discovery a context it cancelled at once, so
// the first refresh failed before any request).
func TestAuthorizedUserADC(t *testing.T) {
	tok := newTokenServer(t, 1)
	adc(t, authorizedUser(tok.URL))
	ls := newListServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(ctx, Config{Endpoint: ls.URL + "/storage/v1/", MaxAttempts: 1})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if src, anon := c.Credentials(); anon || src != "application default credentials" {
		t.Fatalf("credentials %q anonymous=%v", src, anon)
	}
	for range 2 {
		p, err := c.List(context.Background(), &ListInput{Bucket: "b"})
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Items) != 1 {
			t.Fatalf("items %v", p.Items)
		}
	}
	if got := ls.seen(); len(got) != 2 || got[0] != "Bearer tok-1" || got[1] != "Bearer tok-2" {
		t.Fatalf("Authorization headers %q, want the first token then the refreshed one", got)
	}
	if n := tok.refreshes.Load(); n != 2 {
		t.Fatalf("%d token refreshes, want 2", n)
	}
}

// TestCredentialsFileLifetime: an explicit authorized-user file behaves
// the same; a long-lived token is fetched once.
func TestCredentialsFileLifetime(t *testing.T) {
	tok := newTokenServer(t, 3600)
	f := adc(t, authorizedUser(tok.URL))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	ls := newListServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(ctx, Config{Endpoint: ls.URL + "/storage/v1/", CredentialsFile: f, MaxAttempts: 1})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := c.List(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := ls.seen(); len(got) != 3 || got[2] != "Bearer tok-1" {
		t.Fatalf("Authorization headers %q", got)
	}
	if n := tok.refreshes.Load(); n != 1 {
		t.Fatalf("%d token refreshes, want 1 (the token is reused)", n)
	}
}

// TestSlowRefreshBounded: a token endpoint slower than the request timeout
// fails each attempt at the timeout, counted as a timeout and retried; the
// request ends bounded by the attempts and is a credential failure; no
// listing request goes out unauthenticated.
func TestSlowRefreshBounded(t *testing.T) {
	tok := newTokenServer(t, 3600)
	tok.delay = 10 * time.Second
	adc(t, authorizedUser(tok.URL))
	ls := newListServer(t)
	c, err := New(context.Background(), Config{Endpoint: ls.URL + "/storage/v1/", RequestTimeout: 150 * time.Millisecond,
		MaxAttempts: 2, BaseBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.List(context.Background(), &ListInput{Bucket: "b"})
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("List took %s with a 150 ms request timeout", d)
	}
	var ex *ExhaustedError
	var ce *CredentialError
	if !errors.As(err, &ex) || ex.Attempts != 2 || !errors.As(err, &ce) || !ce.Timeout {
		t.Fatalf("err %v, want two attempts exhausted on a credential timeout", err)
	}
	if Classify(err) != objhttp.ClassTimeout || !objhttp.Requeue(Classify)(err) {
		t.Fatalf("class %q: an exhausted token timeout is a requeued timeout, not a stop", Classify(err))
	}
	if n := c.Counters(); n.Timeouts != 2 || n.Retries != 1 || n.Attempts != 0 || n.InflightMax != 0 {
		t.Fatalf("counters %+v, want two timeouts (one retried) and no HTTP attempt", n)
	}
	if len(ls.seen()) != 0 {
		t.Fatalf("listing requests %q without a token", ls.seen())
	}
}

// TestRefusedGrant: a token endpoint that refuses the grant (revoked
// credentials) stops the request at once, not retried and not anonymous.
func TestRefusedGrant(t *testing.T) {
	tok := newTokenServer(t, 3600)
	tok.status = 400
	adc(t, authorizedUser(tok.URL))
	ls := newListServer(t)
	c, err := New(context.Background(), Config{Endpoint: ls.URL + "/storage/v1/", BaseBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &ListInput{Bucket: "b"})
	var ce *CredentialError
	var ex *ExhaustedError
	if !errors.As(err, &ce) || ce.Transient || errors.As(err, &ex) {
		t.Fatalf("err %v, want a non-transient credential error", err)
	}
	if Classify(err) != objhttp.ClassAuth {
		t.Fatalf("class %q, want auth", Classify(err))
	}
	if n := tok.refreshes.Load(); n != 1 || len(ls.seen()) != 0 {
		t.Fatalf("%d refreshes, %d listing requests; want 1 and 0", n, len(ls.seen()))
	}
}

// TestADCConfiguredButBroken: credentials that do not load or are not
// found refuse the client with a credential error instead of listing
// anonymously.
func TestADCConfiguredButBroken(t *testing.T) {
	for _, c := range []struct{ name, body string }{
		{"malformed JSON", `{"type":"authorized_user",`},
		{"unknown type", `{"type":"nonsense"}`},
	} {
		t.Run("env "+c.name, func(t *testing.T) {
			adc(t, c.body)
			_, err := New(context.Background(), Config{})
			var ce *CredentialError
			if !errors.As(err, &ce) {
				t.Fatalf("err %v, want a credential error", err)
			}
		})
		t.Run("gcloud file "+c.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("HOME", dir)
			t.Setenv("APPDATA", filepath.Join(dir, ".config"))
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
			if err := os.MkdirAll(filepath.Join(dir, ".config", "gcloud"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, ".config", "gcloud", "application_default_credentials.json"), []byte(c.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := New(context.Background(), Config{})
			var ce *CredentialError
			if !errors.As(err, &ce) {
				t.Fatalf("err %v, want a credential error", err)
			}
		})
	}
	t.Run("missing file", func(t *testing.T) {
		adc(t, "")
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "absent.json"))
		var ce *CredentialError
		if _, err := New(context.Background(), Config{}); !errors.As(err, &ce) {
			t.Fatalf("err %v, want a credential error", err)
		}
		if _, err := New(context.Background(), Config{CredentialsFile: filepath.Join(t.TempDir(), "absent.json")}); !errors.As(err, &ce) {
			t.Fatalf("explicit file: err %v, want a credential error", err)
		}
	})
	// When the library's search finds nothing (no file, no gcloud login,
	// no metadata server), the client is refused: never anonymous without
	// Anonymous. The search is injected, so the outcome does not depend on
	// the host running the test.
	t.Run("nothing found", func(t *testing.T) {
		old := findCredentials
		t.Cleanup(func() { findCredentials = old })
		findCredentials = func(context.Context, ...string) (*google.Credentials, error) {
			return nil, errors.New("google: could not find default credentials")
		}
		_, err := New(context.Background(), Config{})
		var ce *CredentialError
		if !errors.As(err, &ce) || ce.Transient {
			t.Fatalf("err %v, want a credential error (auth)", err)
		}
	})
}

// slowTokens takes 200 ms per token and records the calls and the most
// concurrent ones.
type slowTokens struct{ calls, now, most atomic.Int64 }

func (s *slowTokens) Token() (*oauth2.Token, error) {
	s.calls.Add(1)
	n := s.now.Add(1)
	defer s.now.Add(-1)
	for m := s.most.Load(); n > m && !s.most.CompareAndSwap(m, n); m = s.most.Load() {
	}
	time.Sleep(200 * time.Millisecond)
	return &oauth2.Token{AccessToken: "t", Expiry: time.Now().Add(-time.Second)}, nil // expired: every wait asks again
}

// TestTokenWaitersCoalesce: attempts that give up waiting for a slow token
// source leave no goroutine behind each; all waiters share one outstanding
// call, so the source is never called concurrently.
func TestTokenWaitersCoalesce(t *testing.T) {
	ts := &slowTokens{}
	c, err := New(context.Background(), Config{Endpoint: "http://127.0.0.1:1/storage/v1/", TokenSource: ts})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				_, _ = c.token(ctx)
				cancel()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if ts.most.Load() != 1 || ts.calls.Load() > int64(elapsed/(200*time.Millisecond))+2 {
		t.Fatalf("400 waits over %s: %d token calls, at most %d at once", elapsed, ts.calls.Load(), ts.most.Load())
	}
}

// TestComputeTokenFromSearch: on a Google Cloud machine (here a fake
// metadata server named by GCE_METADATA_HOST) the client library's own
// search finds the compute token source: the client uses it (never a
// discovery of its own, never anonymity), and a request's token is the
// metadata server's. How long the library reuses a token is its own
// policy (it refreshes ahead of expiry in the background).
func TestComputeTokenFromSearch(t *testing.T) {
	var calls atomic.Int64
	md := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/token") {
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		w.Header().Set("Metadata-Flavor", "Google")
		fmt.Fprint(w, `{"access_token":"t","expires_in":120,"token_type":"Bearer"}`)
	}))
	defer md.Close()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("APPDATA", dir)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(md.URL, "http://"))
	c, err := New(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if src, anon := c.Credentials(); anon || src != "application default credentials" {
		t.Fatalf("source %q, anonymous %v", src, anon)
	}
	tok, err := c.token(context.Background())
	if err != nil || tok.AccessToken != "t" || calls.Load() == 0 {
		t.Fatalf("token %+v, err %v, %d metadata token calls", tok, err, calls.Load())
	}
}

// TestAnonymousPrecedence: Anonymous sends no credentials, whatever the
// environment configures (here working ADC, whose token endpoint is never
// asked); it is the only way a request goes unauthenticated.
func TestAnonymousPrecedence(t *testing.T) {
	tok := newTokenServer(t, 3600)
	adc(t, authorizedUser(tok.URL))
	var auth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		fmt.Fprint(w, `{"kind":"storage#objects"}`)
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if src, anon := c.Credentials(); !anon || src != "anonymous" || auth.Load().(string) != "" || tok.refreshes.Load() != 0 {
		t.Fatalf("credentials %q anonymous=%v, Authorization %q, %d token calls", src, anon, auth.Load(), tok.refreshes.Load())
	}
}
