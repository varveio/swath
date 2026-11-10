// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// recorder serves an empty objects envelope and records each request.
type recorder struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []*http.Request
}

func newRecorder(t *testing.T) *recorder {
	r := &recorder{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.reqs = append(r.reqs, req)
		r.mu.Unlock()
		fmt.Fprint(w, `{"kind":"storage#objects"}`)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recorder) last() *http.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reqs[len(r.reqs)-1]
}

// addressed records the URL each request was addressed to, before toServer
// rewrites its origin to the test server.
type addressed struct {
	toServer
	mu   sync.Mutex
	last *url.URL
}

func (a *addressed) RoundTrip(req *http.Request) (*http.Response, error) {
	a.mu.Lock()
	a.last = new(url.URL)
	*a.last = *req.URL
	a.mu.Unlock()
	return a.toServer.RoundTrip(req)
}

// TestRequests: the listing inputs swath maps. By default a listing is a
// GET addressed to https://storage.googleapis.com/storage/v1/b/BUCKET/o;
// each set input (offsets, the continuation token, the delimiter, the page
// size) reaches its parameter and an unset one is absent; the bucket's
// userProject is sent when billed; projection=noAcl and the production
// fields selector are always sent, and a generations listing sends
// versions=true with a selector that asks for timeDeleted as well.
// Serialization the generated client owns (alt, prettyPrint, escaping) is
// not asserted.
func TestRequests(t *testing.T) {
	if versionFields == fields || !strings.Contains(versionFields, "timeDeleted") || strings.Contains(fields, "timeDeleted") {
		t.Fatal("the versions field mask must add timeDeleted, and only it")
	}
	rec := newRecorder(t)
	u, _ := url.Parse(rec.URL)
	inputs := []string{"prefix", "delimiter", "startOffset", "endOffset", "pageToken", "maxResults", "versions", "userProject"}
	for _, project := range []string{"", "billed-p"} {
		to := &addressed{toServer: toServer{u}}
		c, err := New(context.Background(), Config{Anonymous: true, UserProject: project, HTTPClient: &http.Client{Transport: to}})
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range []ListInput{
			{},
			{Prefix: "p", Delimiter: "/", StartOffset: "a\x00", EndOffset: "e&=#?", PageToken: "tok/+=", MaxResults: 1000, Versions: true},
		} {
			in.Bucket = "bkt"
			if _, err := c.List(context.Background(), &in); err != nil {
				t.Fatal(err)
			}
			to.mu.Lock()
			sent := to.last
			to.mu.Unlock()
			if origin := sent.Scheme + "://" + sent.Host + sent.EscapedPath(); origin != "https://storage.googleapis.com/storage/v1/b/bkt/o" {
				t.Fatalf("project %q %+v: addressed to %s", project, in, sent)
			}
			want := map[string]string{"projection": "noAcl", "fields": fields}
			set := func(k, v string) {
				if v != "" {
					want[k] = v
				}
			}
			if in.Versions {
				want["fields"], want["versions"] = versionFields, "true"
			}
			set("prefix", in.Prefix)
			set("delimiter", in.Delimiter)
			set("startOffset", in.StartOffset)
			set("endOffset", in.EndOffset)
			set("pageToken", in.PageToken)
			set("userProject", project)
			if in.MaxResults > 0 {
				want["maxResults"] = strconv.Itoa(in.MaxResults)
			}
			req := rec.last()
			q := req.URL.Query()
			if req.Method != http.MethodGet {
				t.Errorf("project %q %+v: %s", project, in, req.Method)
			}
			for k, v := range want {
				if !q.Has(k) || q.Get(k) != v {
					t.Errorf("project %q %+v: query %v, want %s=%q", project, in, q, k, v)
				}
			}
			for _, k := range inputs {
				if _, ok := want[k]; q.Has(k) && !ok {
					t.Errorf("project %q %+v: query %v sends unset %s", project, in, q, k)
				}
			}
		}
	}
}

// TestEndpoint: an endpoint is an http(s) URL with a host and no
// URI-template brace in its path, literal or escaped (the generated client
// expands the request path as a template: an empty expression panics, a
// named one is dropped); the trailing slash is optional; another endpoint
// than Google's gets no fields selector. Each base sends to the path the
// generated client makes of it, stated here as the server receives it:
// escapes kept, except an escaped separator, which is sent as a separator,
// and dot segments removed (docs/internals/providers/gcs.md, "Endpoint").
func TestEndpoint(t *testing.T) {
	for _, ep := range []string{"ftp://host/storage/v1/", "storage/v1/", "http:///storage/v1/", "http://%zz/", "http://host/%zz",
		"http://host/%7B%7D/storage/v1/", "http://host/%7Btenant%7D/storage/v1/", "http://host/%7b%7d/",
		"http://host/{}/storage/v1/", "http://host/{tenant}/storage/v1/", "http://host/a%7D/", "http://host/storage/v1/{"} {
		if _, err := New(context.Background(), Config{Endpoint: ep, Anonymous: true}); err == nil {
			t.Errorf("%q accepted", ep)
		}
	}
	rec := newRecorder(t)
	for base, path := range map[string]string{
		"/storage/v1/":                "/storage/v1/b/bkt/o",
		"/storage/v1":                 "/storage/v1/b/bkt/o",
		"":                            "/b/bkt/o",
		"/custom%20base/v1":           "/custom%20base/v1/b/bkt/o",
		"/tenant%2F":                  "/tenant//b/bkt/o",
		"/proxy%2Ftenant/storage/v1/": "/proxy/tenant/storage/v1/b/bkt/o",
		"/a/../storage/v1/":           "/storage/v1/b/bkt/o",
		"/a/./storage/v1/":            "/a/storage/v1/b/bkt/o",
	} {
		c, err := New(context.Background(), Config{Endpoint: rec.URL + base, Anonymous: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.List(context.Background(), &ListInput{Bucket: "bkt"}); err != nil {
			t.Fatal(err)
		}
		req := rec.last()
		if got, _, _ := strings.Cut(req.RequestURI, "?"); got != path || req.URL.Query().Has("fields") {
			t.Errorf("%q: sent %s, want path %s and no fields", base, req.RequestURI, path)
		}
	}
	if _, err := New(context.Background(), Config{Anonymous: true}); err != nil {
		t.Fatalf("default endpoint: %v", err)
	}
}

// TestEmptyListingCarriesKind: an empty listing under the production
// fields selector still names its kind (the selector asks for it), so the
// client reads it as an empty final page.
func TestEmptyListingCarriesKind(t *testing.T) {
	f := gcstest.NewStrict([]string{"a"})
	u, _ := url.Parse(f.Serve(t))
	c, err := New(context.Background(), Config{Anonymous: true, HTTPClient: &http.Client{Transport: toServer{u}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, versions := range []bool{false, true} {
		p, err := c.List(context.Background(), &ListInput{Bucket: "bkt", Prefix: "none/", Versions: versions})
		if err != nil || len(p.Items) != 0 || p.NextPageToken != "" {
			t.Fatalf("versions %v: %+v, %v", versions, p, err)
		}
	}
}

// found is a credential search that finds ts, or fails with err.
type found struct {
	ts  oauth2.TokenSource
	err error
}

func (f found) find(context.Context, ...string) (*google.Credentials, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &google.Credentials{TokenSource: f.ts}, nil
}

// withTokens builds a client whose credential search finds ts.
func withTokens(cfg Config, ts oauth2.TokenSource) (*Client, error) {
	return newClient(context.Background(), cfg, found{ts: ts}, objhttp.Pool(cfg.Workers, cfg.HTTPClient, nil))
}

type tokens struct{ err error }

func (s tokens) Token() (*oauth2.Token, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &oauth2.Token{AccessToken: "tok", TokenType: "bearer", Expiry: time.Now().Add(time.Hour)}, nil
}

// TestCredentialsAndHeaders: each attempt carries the credentials' shared
// token source's bearer token and swath's product token (prepended to the generated
// client's own User-Agent); a token the source will not issue is a
// credential failure before any request (no attempt counted).
func TestCredentialsAndHeaders(t *testing.T) {
	var auth, agent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, agent = r.Header.Get("Authorization"), r.Header.Get("User-Agent")
		io.WriteString(w, `{"kind":"storage#objects"}`)
	}))
	defer srv.Close()
	c, err := withTokens(Config{Endpoint: srv.URL + "/storage/v1/"}, tokens{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if want := buildinfo.UserAgent() + " " + googleapi.UserAgent; auth != "Bearer tok" || agent != want {
		t.Fatalf("Authorization %q, User-Agent %q", auth, agent)
	}
	c, err = withTokens(Config{Endpoint: srv.URL + "/storage/v1/"}, tokens{errors.New("refused")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &ListInput{Bucket: "b"})
	if objhttp.Classify(err) != objhttp.ClassAuth || c.Counters().Attempts != 0 {
		t.Fatalf("err %v, counters %+v", err, c.Counters())
	}
	if source, anonymous := c.Credentials(); source != "application default credentials" || anonymous {
		t.Fatalf("credentials %q %v", source, anonymous)
	}
}
