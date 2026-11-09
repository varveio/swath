// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// entraServer is a local Microsoft Entra ID (instance discovery, tenant
// metadata, the v2.0 token endpoint issuing tok-N for a client secret,
// valid for expires seconds) and a blob endpoint answering each listing
// with one blob, recording each request's Authorization. Every request
// reaches it through a transport that rewrites the host, so the
// authority (login.test), Microsoft's discovery host and the storage
// endpoint are all this server.
type entraServer struct {
	*httptest.Server
	tokens  atomic.Int64 // token requests
	expires int
	delay   time.Duration // before a token answer
	status  int           // non-zero: the token endpoint answers this
	mu      sync.Mutex
	auths   []string
}

const tenant = "tenant1"

func newEntraServer(t *testing.T, expires int) *entraServer {
	es := &entraServer{expires: expires}
	es.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		auth := "https://login.test/" + tenant
		switch {
		case r.URL.Path == "/common/discovery/instance":
			fmt.Fprintf(w, `{"tenant_discovery_endpoint":%q,"api-version":"1.1","metadata":[{"preferred_network":"login.test","preferred_cache":"login.test","aliases":["login.test"]}]}`,
				auth+"/v2.0/.well-known/openid-configuration")
		case r.URL.Path == "/"+tenant+"/v2.0/.well-known/openid-configuration":
			fmt.Fprintf(w, `{"token_endpoint":%q,"authorization_endpoint":%q,"issuer":%q}`, auth+"/oauth2/v2.0/token", auth+"/oauth2/v2.0/authorize", auth+"/v2.0")
		case r.URL.Path == "/"+tenant+"/oauth2/v2.0/token":
			n := es.tokens.Add(1)
			if err := r.ParseForm(); err != nil || r.Form.Get("client_secret") != "cs" || !strings.Contains(r.Form.Get("scope"), storageScope) {
				w.WriteHeader(400)
				fmt.Fprint(w, `{"error":"invalid_request"}`)
				return
			}
			select {
			case <-time.After(es.delay):
			case <-r.Context().Done():
				return
			}
			if es.status != 0 {
				w.WriteHeader(es.status)
				fmt.Fprint(w, `{"error":"invalid_client","error_description":"AADSTS7000215: Invalid client secret provided."}`)
				return
			}
			fmt.Fprintf(w, `{"token_type":"Bearer","expires_in":%d,"ext_expires_in":%d,"access_token":"tok-%d"}`, es.expires, es.expires, n)
		case strings.HasPrefix(r.URL.Path, "/acct/"):
			es.mu.Lock()
			es.auths = append(es.auths, r.Header.Get(headerAuthorization))
			es.mu.Unlock()
			if r.Header.Get(headerAuthorization) == "" {
				azureError(w, 401, codeNoAuthentication)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, page("", "a"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(es.Close)
	return es
}

func (es *entraServer) seen() []string {
	es.mu.Lock()
	defer es.mu.Unlock()
	return append([]string(nil), es.auths...)
}

// rewrite sends every request to the test server, whatever its host.
type rewrite struct {
	host string
	next http.RoundTripper
}

func (r rewrite) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host = "https", r.host
	return r.next.RoundTrip(req)
}

// clearEntra empties the environment DefaultAzureCredential reads, so the
// developer's own credentials stay out of the tests.
func clearEntra(t *testing.T) {
	for _, v := range append(entraVars, "AZURE_AUTHORITY_HOST", "AZURE_STORAGE_KEY", "AZURE_STORAGE_SAS_TOKEN") {
		t.Setenv(v, "") // restored after the test
		os.Unsetenv(v)  // DefaultAzureCredential refuses an empty AZURE_TOKEN_CREDENTIALS
	}
	t.Setenv("HOME", t.TempDir())
}

// entraEnv configures the environment credential (a client secret) against
// es, and returns a client config whose HTTP client reaches es.
func entraEnv(t *testing.T, es *entraServer) Config {
	clearEntra(t)
	t.Setenv("AZURE_TENANT_ID", tenant)
	t.Setenv("AZURE_CLIENT_ID", "cid")
	t.Setenv("AZURE_CLIENT_SECRET", "cs")
	t.Setenv("AZURE_AUTHORITY_HOST", "https://login.test/")
	u, _ := url.Parse(es.URL)
	return Config{Endpoint: "https://storage.test", Account: "acct",
		HTTPClient: &http.Client{Transport: rewrite{u.Host, es.Client().Transport}}}
}

// fixed is an Entra ID source that builds cred, or fails with err.
type fixed struct {
	cred azcore.TokenCredential
	err  error
}

func (f fixed) credential(*azidentity.DefaultAzureCredentialOptions) (azcore.TokenCredential, error) {
	return f.cred, f.err
}

// withEntra is New's construction with cred as the Entra ID credential.
func withEntra(cred azcore.TokenCredential) build {
	return build{bodyCap: MaxBody, entra: fixed{cred: cred}}
}

// newEntraClient builds a client whose Entra ID credential is cred.
func newEntraClient(t *testing.T, cfg Config, cred azcore.TokenCredential) *Client {
	t.Helper()
	if cfg.Account == "" {
		cfg.Account = "acct"
	}
	c, err := newClient(context.Background(), cfg, withEntra(cred))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestEntraEnvironmentCredential: Entra ID through DefaultAzureCredential
// configured by the environment gets a token at the first request and a
// fresh one when it expires, although the context New ran under has
// ended; the listing carries it as a bearer token.
func TestEntraEnvironmentCredential(t *testing.T) {
	es := newEntraServer(t, 1)
	cfg := entraEnv(t, es)
	cfg.MaxAttempts = 1
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(ctx, cfg)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if src, anon := c.Credentials(); anon || !strings.HasPrefix(src, "Entra ID") {
		t.Fatalf("credentials %q anonymous=%v", src, anon)
	}
	if n := es.tokens.Load(); n != 0 {
		t.Fatalf("%d token requests before the first listing request", n)
	}
	for range 2 {
		if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := es.seen(); len(got) != 2 || got[0] != "Bearer tok-1" || got[1] != "Bearer tok-2" {
		t.Fatalf("Authorization headers %q, want the first token then the refreshed one", got)
	}
}

// TestEntraTokenReused: a long-lived token is fetched once.
func TestEntraTokenReused(t *testing.T) {
	es := newEntraServer(t, 3600)
	cfg := entraEnv(t, es)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := es.seen(); len(got) != 3 || got[2] != "Bearer tok-1" || es.tokens.Load() != 1 {
		t.Fatalf("Authorization headers %q after %d token requests", got, es.tokens.Load())
	}
}

// TestEntraSlowRefreshBounded: a token endpoint slower than the request
// timeout fails each attempt at the timeout, counted as a timeout and
// retried; the request ends bounded by its attempts as a requeued
// credential timeout, and no listing request goes out unauthenticated.
func TestEntraSlowRefreshBounded(t *testing.T) {
	es := newEntraServer(t, 3600)
	es.delay = 10 * time.Second
	cfg := entraEnv(t, es)
	cfg.RequestTimeout, cfg.MaxAttempts, cfg.BaseBackoff = 150*time.Millisecond, 2, time.Millisecond
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.List(context.Background(), &ListInput{Container: "c"})
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("List took %s with a 150 ms request timeout", d)
	}
	var ex *ExhaustedError
	var ce *CredentialError
	if !errors.As(err, &ex) || ex.Attempts != 2 || !errors.As(err, &ce) || !ce.Timeout {
		t.Fatalf("err %v, want two attempts exhausted on a credential timeout", err)
	}
	if Classify(err) != objhttp.ClassTimeout || !objhttp.Requeue(Classify)(err) {
		t.Fatalf("class %q: an exhausted token timeout is a requeued timeout", Classify(err))
	}
	if n := c.Counters(); n.Timeouts != 2 || n.Retries != 1 || n.Attempts != 0 {
		t.Fatalf("counters %+v, want two timeouts (one retried) and no HTTP attempt", n)
	}
	if len(es.seen()) != 0 {
		t.Fatalf("listing requests %q without a token", es.seen())
	}
}

// TestEntraRefusedGrant: a token endpoint that refuses the grant (a wrong
// secret) stops the request at once: one token request, not retried, no
// listing request, class auth.
func TestEntraRefusedGrant(t *testing.T) {
	es := newEntraServer(t, 3600)
	es.status = 401
	cfg := entraEnv(t, es)
	cfg.BaseBackoff = time.Millisecond
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &ListInput{Container: "c"})
	var ce *CredentialError
	var ex *ExhaustedError
	if !errors.As(err, &ce) || ce.Transient || errors.As(err, &ex) || Classify(err) != objhttp.ClassAuth {
		t.Fatalf("err %v (class %q), want a non-transient credential error", err, Classify(err))
	}
	if n := es.tokens.Load(); n != 1 || len(es.seen()) != 0 {
		t.Fatalf("%d token requests, %d listing requests; want 1 and 0", n, len(es.seen()))
	}
}

// TestEntraConfiguredButBroken: a credential the environment configures
// but that cannot work (a client id without its secret or tenant) is a
// credential error at the first request, never anonymity.
func TestEntraConfiguredButBroken(t *testing.T) {
	es := newEntraServer(t, 3600)
	cfg := entraEnv(t, es)
	os.Unsetenv("AZURE_CLIENT_SECRET")
	os.Unsetenv("AZURE_TENANT_ID")
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "EnvironmentCredential")
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, anon := c.Credentials(); anon {
		t.Fatal("anonymous with a configured credential")
	}
	_, err = c.List(context.Background(), &ListInput{Container: "c"})
	var ce *CredentialError
	if !errors.As(err, &ce) || Classify(err) != objhttp.ClassAuth || len(es.seen()) != 0 {
		t.Fatalf("err %v (class %q), %d listing requests", err, Classify(err), len(es.seen()))
	}
}

type countingCredential struct {
	calls, active, peak atomic.Int64
	delay               time.Duration
	err                 error
}

func (c *countingCredential) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.calls.Add(1)
	n := c.active.Add(1)
	defer c.active.Add(-1)
	for p := c.peak.Load(); n > p && !c.peak.CompareAndSwap(p, n); p = c.peak.Load() {
	}
	time.Sleep(c.delay)
	if c.err != nil {
		return azcore.AccessToken{}, c.err
	}
	return azcore.AccessToken{Token: "tok", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// TestTokenWaitersCoalesce: concurrent requests share one outstanding
// token call (tokensource.Shared), and the token is reused.
func TestTokenWaitersCoalesce(t *testing.T) {
	var bearer atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(headerAuthorization) == "Bearer tok" {
			bearer.Add(1)
		}
		fmt.Fprint(w, page(""))
	}))
	defer srv.Close()
	cred := &countingCredential{delay: 100 * time.Millisecond}
	c := newEntraClient(t, Config{Endpoint: srv.URL}, cred)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if cred.calls.Load() != 1 || cred.peak.Load() != 1 || bearer.Load() != 20 {
		t.Fatalf("%d token calls (peak %d concurrent), %d bearer requests", cred.calls.Load(), cred.peak.Load(), bearer.Load())
	}
}

// TestEntraDefaultChain: with no SAS, key or supplied credential, the
// client always builds DefaultAzureCredential (never a discovery of its
// own) and never lists unauthenticated. Its token is asked for per
// attempt, before the request is sent: a token found is sent; a chain
// that finds no credential (azidentity's own unavailable error, from the
// real chain in an empty environment) and a refused grant fail the first
// request with class auth and send no listing request; a token
// endpoint's 503 is retried (transport); a chain that does not build
// refuses the client.
func TestEntraDefaultChain(t *testing.T) {
	refusedErr := &azidentity.AuthenticationFailedError{RawResponse: &http.Response{StatusCode: 401}}
	for _, x := range []struct {
		name     string
		cred     azcore.TokenCredential // nil: the real chain, environment credential only
		buildErr error
		fails    bool          // New refuses
		class    objhttp.Class // the first request's class; "" lists
	}{
		{name: "found", cred: &countingCredential{}},
		{name: "absent", class: objhttp.ClassAuth},
		{name: "refused", cred: &countingCredential{err: refusedErr}, class: objhttp.ClassAuth},
		{name: "token endpoint 503", cred: &countingCredential{err: &azidentity.AuthenticationFailedError{RawResponse: &http.Response{StatusCode: 503}}}, class: objhttp.ClassTransport},
		{name: "does not build", buildErr: errors.New("invalid AZURE_TOKEN_CREDENTIALS"), fails: true},
	} {
		t.Run(x.name, func(t *testing.T) {
			clearEntra(t)
			b := build{bodyCap: MaxBody, entra: fixed{x.cred, x.buildErr}}
			if x.cred == nil && x.buildErr == nil {
				t.Setenv("AZURE_TOKEN_CREDENTIALS", "EnvironmentCredential")
				b.entra = defaultChain{}
			}
			var listed atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				listed.Add(1)
				fmt.Fprint(w, page(""))
			}))
			defer srv.Close()
			c, err := newClient(context.Background(), Config{Endpoint: srv.URL, Account: "acct", MaxAttempts: 1}, b)
			var ce *CredentialError
			if x.fails {
				if !errors.As(err, &ce) || Classify(err) != objhttp.ClassAuth {
					t.Fatalf("err %v, want a credential error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if src, anon := c.Credentials(); anon || !strings.Contains(src, "DefaultAzureCredential") {
				t.Fatalf("credentials %q anonymous=%v", src, anon)
			}
			_, err = c.List(context.Background(), &ListInput{Container: "c"})
			if x.class == "" {
				if err != nil || listed.Load() != 1 {
					t.Fatalf("list: %v, %d requests", err, listed.Load())
				}
				return
			}
			if !errors.As(err, &ce) || Classify(err) != x.class || listed.Load() != 0 {
				t.Fatalf("first request: %v (class %q, %d listing requests), want a %s credential error and none sent", err, Classify(err), listed.Load(), x.class)
			}
		})
	}
}

// TestAnonymousPrecedence: Anonymous sends nothing, whatever else is
// configured, and notes the credentials it ignores.
func TestAnonymousPrecedence(t *testing.T) {
	clearEntra(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(headerAuthorization) != "" || r.URL.Query().Has("sig") {
			t.Errorf("credentials sent: %v ?%s", r.Header, r.URL.RawQuery)
		}
		fmt.Fprint(w, page(""))
	}))
	defer srv.Close()
	c := newEntraClient(t, Config{Endpoint: srv.URL, Anonymous: true, SAS: "sig=x", SharedKey: testKey}, &countingCredential{})
	if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
		t.Fatal(err)
	}
	if src, anon := c.Credentials(); !anon || src != "anonymous" || !strings.Contains(c.Note(), "ignoring") {
		t.Fatalf("credentials %q anonymous=%v note %q", src, anon, c.Note())
	}
	if d := newTestClient(t, Config{Anonymous: true}); d.Note() != "" {
		t.Fatalf("note %q with nothing to ignore", d.Note())
	}
}
