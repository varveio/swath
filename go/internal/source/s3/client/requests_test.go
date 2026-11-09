// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// recorder answers without a network and keeps each request's URL: a
// ListVersionsResult to a versions request, okPage to the others, after
// failing the first fail requests with a 503.
type recorder struct {
	fail   int
	mu     sync.Mutex
	urls   []*url.URL
	agents []string
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.urls = append(r.urls, req.URL)
	r.agents = append(r.agents, req.Header.Get("User-Agent"))
	n := len(r.urls)
	r.mu.Unlock()
	status, body := 200, okPage
	switch {
	case n <= r.fail:
		status, body = 503, `<Error><Code>SlowDown</Code></Error>`
	case req.URL.Query().Has("versions"):
		body = `<ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>`
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}

// TestUserAgent records the finished header through Guard.Next, including
// a retry, a continuation and a versions request.
func TestUserAgent(t *testing.T) {
	s3test.NoCredentials(t)
	rec := &recorder{fail: 1}
	c, err := New(context.Background(), Config{Region: "us-east-1", Anonymous: true,
		HTTPClient: &http.Client{Transport: rec}, Retry: objhttp.Retry{MaxAttempts: 2}})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []*ListInput{{Bucket: "b"}, {Bucket: "b", ContinuationToken: "next"}} {
		if _, err := c.ListObjectsV2(context.Background(), in); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if len(rec.agents) != 4 {
		t.Fatalf("%d requests, want 4", len(rec.agents))
	}
	for _, agent := range rec.agents {
		if token := buildinfo.UserAgent(); !strings.HasPrefix(agent, token+" ") ||
			!strings.Contains(agent, "aws-sdk-go-v2/") || strings.Count(agent, token) != 1 {
			t.Errorf("User-Agent %q: want one leading product token and the SDK product", agent)
		}
	}
	t.Logf("User-Agent: %s", rec.agents[0])
}

// routeOf is a request URL's route: scheme, host and escaped path.
func routeOf(u *url.URL) string { return u.Scheme + "://" + u.Host + u.EscapedPath() }

// TestRequestURLs: the endpoint options swath maps reach the SDK, and the
// request goes where the provider's recorded identity says it does
// (register.go endpointCanonical): spellings that share one identity send
// to one URL, and spellings it keeps apart send to different ones. A
// virtual-host endpoint keeps its path byte for byte (/s3 and /s3/ are two
// request paths); path-style and a pathless endpoint fold one trailing
// slash; escapes and dot segments are sent as given. Recorded under the
// Guard.
func TestRequestURLs(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range []struct {
		name  string
		cfg   Config
		route string
	}{
		{"region", Config{Region: "eu-west-1"}, "https://bkt.s3.eu-west-1.amazonaws.com/"},
		{"path-style", Config{Region: "us-east-1", PathStyle: true}, "https://s3.us-east-1.amazonaws.com/bkt"},
		{"FIPS and dual-stack", Config{Region: "us-east-1", FIPS: true, DualStack: true}, "https://bkt.s3-fips.dualstack.us-east-1.amazonaws.com/"},
		{"virtual-host, path with a slash", Config{Endpoint: "https://minio.example/s3/"}, "https://bkt.minio.example/s3/"},
		{"virtual-host, path without a slash", Config{Endpoint: "https://minio.example/s3"}, "https://bkt.minio.example/s3"},
		{"virtual-host, no path", Config{Endpoint: "https://minio.example"}, "https://bkt.minio.example/"},
		{"virtual-host, root path", Config{Endpoint: "https://minio.example/"}, "https://bkt.minio.example/"},
		{"path-style, path with a slash", Config{Endpoint: "https://minio.example/s3/", PathStyle: true}, "https://minio.example/s3/bkt"},
		{"path-style, path without a slash", Config{Endpoint: "https://minio.example/s3", PathStyle: true}, "https://minio.example/s3/bkt"},
		{"path-style, escaped path", Config{Endpoint: "https://minio.example/a%20b/", PathStyle: true}, "https://minio.example/a%20b/bkt"},
		{"path-style, dot segments", Config{Endpoint: "https://minio.example/x/../y/", PathStyle: true}, "https://minio.example/x/../y/bkt"},
	} {
		rec := &recorder{}
		cfg := c.cfg
		if cfg.Region == "" {
			cfg.Region = "us-east-1"
		}
		cfg.Anonymous, cfg.HTTPClient = true, &http.Client{Transport: rec}
		if _, err := newTest(t, cfg).ListObjectsV2(context.Background(), &ListInput{Bucket: "bkt"}); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := routeOf(rec.urls[0]); got != c.route {
			t.Errorf("%s: request to %s, want %s", c.name, got, c.route)
		}
	}
}

// TestRequestOptions: each listing input swath sets reaches its query
// parameter, encoding-type=url on every request (the scanner decodes the
// keys once), and an unset input is absent.
func TestRequestOptions(t *testing.T) {
	s3test.NoCredentials(t)
	rec := &recorder{}
	cl := newTest(t, Config{Region: "us-east-1", Anonymous: true, HTTPClient: &http.Client{Transport: rec}})
	ctx := context.Background()
	if _, err := cl.ListObjectsV2(ctx, &ListInput{Bucket: "bkt", Prefix: "p/", StartAfter: "p/a b", MaxKeys: 7, FetchOwner: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.ListObjectsV2(ctx, &ListInput{Bucket: "bkt", ContinuationToken: "1/tok+==", Delimiter: "/"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.ListObjectVersions(ctx, &VersionsInput{Bucket: "bkt", KeyMarker: "k%", VersionIDMarker: "null"}); err != nil {
		t.Fatal(err)
	}
	inputs := []string{"prefix", "start-after", "max-keys", "fetch-owner", "continuation-token", "delimiter", "key-marker", "version-id-marker"}
	for i, want := range []map[string]string{
		{"list-type": "2", "encoding-type": "url", "prefix": "p/", "start-after": "p/a b", "max-keys": "7", "fetch-owner": "true"},
		{"list-type": "2", "encoding-type": "url", "continuation-token": "1/tok+==", "delimiter": "/"},
		{"versions": "", "encoding-type": "url", "key-marker": "k%", "version-id-marker": "null"},
	} {
		q := rec.urls[i].Query()
		for k, v := range want {
			if !q.Has(k) || q.Get(k) != v {
				t.Errorf("request %d: query %s, want %s=%q", i, rec.urls[i].RawQuery, k, v)
			}
		}
		for _, k := range inputs {
			if _, set := want[k]; q.Has(k) && !set {
				t.Errorf("request %d: query %s sends unset %s", i, rec.urls[i].RawQuery, k)
			}
		}
	}
}

// TestCustomEndpointIgnoresEndpointFlags: with a custom endpoint, FIPS and
// dual-stack are not used (they select among AWS endpoints): an
// S3-compatible store lists, path-style, with either flag.
func TestCustomEndpointIgnoresEndpointFlags(t *testing.T) {
	s3test.NoCredentials(t)
	url := s3test.Bucket(t, []string{"a", "b/c"})
	for _, flags := range []Config{{FIPS: true}, {DualStack: true}, {FIPS: true, DualStack: true}} {
		cfg := testConfig(url)
		cfg.FIPS, cfg.DualStack = flags.FIPS, flags.DualStack
		pg, err := newTest(t, cfg).ListObjectsV2(context.Background(), &ListInput{Bucket: s3test.Name})
		if err != nil || len(pg.Contents) != 2 {
			t.Fatalf("%+v: %v %+v", flags, err, pg)
		}
	}
}

func TestEndpointRefusal(t *testing.T) {
	for _, ep := range []string{"http://%zz/", "http://host/%zz", "ftp://host/", "host/path", "http:///path"} {
		t.Run(ep, func(t *testing.T) {
			c, err := New(t.Context(), Config{Endpoint: ep, Anonymous: true})
			if c != nil || err == nil || !strings.Contains(err.Error(), "endpoint") {
				t.Fatalf("client %v, error %v", c, err)
			}
		})
	}
}
