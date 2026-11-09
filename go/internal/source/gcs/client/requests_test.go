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
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
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

// shapes are listing inputs with keys that need escaping, and generations
// listings.
var shapes = []ListInput{
	{MaxResults: 7},
	{},
	{Prefix: "data/year=2021/", MaxResults: 100},
	{StartOffset: "a/x", EndOffset: "data/year=2020/part-00100.parquet\x00", MaxResults: 50},
	{Delimiter: "/", MaxResults: 3},
	{Prefix: "a/", Delimiter: "/"},
	{StartOffset: "pct%41", EndOffset: "sp ace"},
	{StartOffset: "ctl\x01", MaxResults: 1},
	{Versions: true, MaxResults: 9},
	{Versions: true, StartOffset: "a\x00", Delimiter: "/", MaxResults: 4},
	{Prefix: "p", Delimiter: "/", StartOffset: "s+%", EndOffset: "e&=#?", PageToken: "tok/+=", MaxResults: 1000, Versions: true},
}

// TestRequests: on Google's endpoint, each listing input is one GET of
// /storage/v1/b/BUCKET/o carrying exactly its parameters, set ones only:
// the production fields selector (a generations listing's with versions),
// projection=noAcl, the bucket's userProject when billed, and the
// generated client's alt=json and prettyPrint=false.
func TestRequests(t *testing.T) {
	rec := newRecorder(t)
	u, _ := url.Parse(rec.URL)
	for _, project := range []string{"", "billed-p"} {
		c, err := New(context.Background(), Config{Anonymous: true, UserProject: project, HTTPClient: &http.Client{Transport: toServer{u}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range shapes {
			in.Bucket = "bkt"
			if _, err := c.List(context.Background(), &in); err != nil {
				t.Fatal(err)
			}
			req := rec.last()
			want := url.Values{"alt": {"json"}, "prettyPrint": {"false"}, "projection": {"noAcl"}, "fields": {fields}}
			set := func(k, v string) {
				if v != "" {
					want.Set(k, v)
				}
			}
			if in.Versions {
				want.Set("fields", versionFields)
				want.Set("versions", "true")
			}
			set("prefix", in.Prefix)
			set("delimiter", in.Delimiter)
			set("startOffset", in.StartOffset)
			set("endOffset", in.EndOffset)
			set("pageToken", in.PageToken)
			set("userProject", project)
			if in.MaxResults > 0 {
				want.Set("maxResults", strconv.Itoa(in.MaxResults))
			}
			if req.Method != http.MethodGet || req.URL.EscapedPath() != "/storage/v1/b/bkt/o" || !reflect.DeepEqual(req.URL.Query(), want) {
				t.Fatalf("project %q %+v: %s %s, want query %v", project, in, req.Method, req.URL, want)
			}
		}
	}
}

// TestEndpoint: an endpoint is an http(s) URL with a host and no
// URI-template brace in its path, literal or escaped (the generated client
// expands the request path as a template: an empty expression panics, a
// named one is dropped); the trailing slash is optional; the base path is
// kept as given, escapes included; another endpoint than Google's gets no
// fields selector.
func TestEndpoint(t *testing.T) {
	for _, ep := range []string{"ftp://host/storage/v1/", "storage/v1/", "http:///storage/v1/", "http://%zz/",
		"http://host/%7B%7D/storage/v1/", "http://host/%7Btenant%7D/storage/v1/", "http://host/%7b%7d/",
		"http://host/{}/storage/v1/", "http://host/{tenant}/storage/v1/", "http://host/a%7D/", "http://host/storage/v1/{"} {
		if _, err := New(context.Background(), Config{Endpoint: ep, Anonymous: true}); err == nil {
			t.Errorf("%q accepted", ep)
		}
	}
	rec := newRecorder(t)
	for base, path := range map[string]string{
		"/storage/v1/":      "/storage/v1/b/bkt/o",
		"/storage/v1":       "/storage/v1/b/bkt/o",
		"/custom%20base/v1": "/custom%20base/v1/b/bkt/o",
	} {
		c, err := New(context.Background(), Config{Endpoint: rec.URL + base, Anonymous: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.List(context.Background(), &ListInput{Bucket: "bkt"}); err != nil {
			t.Fatal(err)
		}
		if got := rec.last().URL; got.EscapedPath() != path || got.Query().Has("fields") {
			t.Errorf("%s: sent %s, want path %s and no fields", base, got, path)
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

type tokens struct{ err error }

func (s tokens) Token() (*oauth2.Token, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &oauth2.Token{AccessToken: "tok", TokenType: "bearer", Expiry: time.Now().Add(time.Hour)}, nil
}

// TestCredentialsAndHeaders: each attempt carries the shared token source's
// bearer token and swath's product token (prepended to the generated
// client's own User-Agent); a token the source will not issue is a
// credential failure before any request (no attempt counted).
func TestCredentialsAndHeaders(t *testing.T) {
	var auth, agent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, agent = r.Header.Get("Authorization"), r.Header.Get("User-Agent")
		io.WriteString(w, `{"kind":"storage#objects"}`)
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", TokenSource: tokens{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if want := buildinfo.UserAgent() + " " + googleapi.UserAgent; auth != "Bearer tok" || agent != want {
		t.Fatalf("Authorization %q, User-Agent %q", auth, agent)
	}
	c, err = New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", TokenSource: tokens{errors.New("refused")}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &ListInput{Bucket: "b"})
	if Classify(err) != objhttp.ClassAuth || c.Counters().Attempts != 0 {
		t.Fatalf("err %v, counters %+v", err, c.Counters())
	}
	if source, anonymous := c.Credentials(); source != "token source" || anonymous {
		t.Fatalf("credentials %q %v", source, anonymous)
	}
}
