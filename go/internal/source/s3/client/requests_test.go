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
		HTTPClient: &http.Client{Transport: rec}, MaxAttempts: 2})
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

// TestRequestURLs: for every endpoint form the provider supports, every
// request goes to the URL the table names (scheme, host and escaped path):
// the first page, a retried attempt, a continuation and a versions page;
// the query carries the listing's parameters, encoding-type=url included.
// The requests are recorded under the Guard. A FIPS region alias is the
// SDK's to resolve: it sends to the region's FIPS host.
func TestRequestURLs(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range []struct {
		name   string
		cfg    Config
		bucket string
		route  string
	}{
		{"default endpoint", Config{Region: "eu-west-1"}, "bkt", "https://bkt.s3.eu-west-1.amazonaws.com/"},
		{"dotted bucket", Config{Region: "us-east-1"}, "a.b", "https://s3.us-east-1.amazonaws.com/a.b"},
		{"path-style", Config{Region: "us-east-1", PathStyle: true}, "bkt", "https://s3.us-east-1.amazonaws.com/bkt"},
		{"custom IP endpoint", Config{Region: "us-east-1", Endpoint: "http://127.0.0.1:9000"}, "bkt", "http://127.0.0.1:9000/bkt"},
		{"custom IPv6 endpoint", Config{Region: "us-east-1", Endpoint: "http://[::1]:9000"}, "bkt", "http://[::1]:9000/bkt"},
		{"custom endpoint with a path", Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/"}, "bkt", "https://bkt.minio.example/s3/"},
		{"custom endpoint with a path, path-style", Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/", PathStyle: true}, "bkt",
			"https://minio.example/s3/bkt"},
		{"custom endpoint with a path, no slash", Config{Region: "us-east-1", Endpoint: "https://minio.example/s3", PathStyle: true}, "bkt",
			"https://minio.example/s3/bkt"},
		{"custom endpoint with an escaped path", Config{Region: "us-east-1", Endpoint: "https://minio.example/a%20b/", PathStyle: true}, "bkt",
			"https://minio.example/a%20b/bkt"},
		{"custom endpoint with dot segments", Config{Region: "us-east-1", Endpoint: "https://minio.example/x/../y/", PathStyle: true}, "bkt",
			"https://minio.example/x/../y/bkt"},
		{"FIPS", Config{Region: "us-gov-west-1", FIPS: true}, "bkt", "https://bkt.s3-fips.us-gov-west-1.amazonaws.com/"},
		{"dual-stack", Config{Region: "eu-west-1", DualStack: true}, "bkt", "https://bkt.s3.dualstack.eu-west-1.amazonaws.com/"},
		{"FIPS and dual-stack", Config{Region: "us-east-1", FIPS: true, DualStack: true}, "bkt", "https://bkt.s3-fips.dualstack.us-east-1.amazonaws.com/"},
		{"cn-north-1", Config{Region: "cn-north-1"}, "bkt", "https://bkt.s3.cn-north-1.amazonaws.com.cn/"},
		{"FIPS alias fips-us-east-1", Config{Region: "fips-us-east-1"}, "bkt", "https://bkt.s3-fips.us-east-1.amazonaws.com/"},
		{"FIPS alias us-east-1-fips", Config{Region: "us-east-1-fips"}, "bkt", "https://bkt.s3-fips.us-east-1.amazonaws.com/"},
	} {
		rec := &recorder{fail: 1}
		cfg := c.cfg
		cfg.Anonymous, cfg.HTTPClient, cfg.MaxAttempts = true, &http.Client{Transport: rec}, 2
		cl := newTest(t, cfg)
		ctx := context.Background()
		// A 503, then the first page: the retried attempt.
		if _, err := cl.ListObjectsV2(ctx, &ListInput{Bucket: c.bucket, Prefix: "p/", StartAfter: "p/a b", MaxKeys: 7, FetchOwner: true}); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if _, err := cl.ListObjectsV2(ctx, &ListInput{Bucket: c.bucket, ContinuationToken: "1/tok+==", Delimiter: "/"}); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if _, err := cl.ListObjectVersions(ctx, &VersionsInput{Bucket: c.bucket, KeyMarker: "k%", VersionIDMarker: "null"}); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(rec.urls) != 4 {
			t.Fatalf("%s: %d requests", c.name, len(rec.urls))
		}
		for i, u := range rec.urls {
			if routeOf(u) != c.route {
				t.Errorf("%s: request %d to %s, want %s", c.name, i, u, c.route)
			}
		}
		for i, want := range []map[string]string{
			{"list-type": "2", "encoding-type": "url", "prefix": "p/", "start-after": "p/a b", "max-keys": "7", "fetch-owner": "true"},
			{"list-type": "2", "encoding-type": "url", "prefix": "p/", "start-after": "p/a b", "max-keys": "7", "fetch-owner": "true"},
			{"list-type": "2", "encoding-type": "url", "continuation-token": "1/tok+==", "delimiter": "/", "start-after": ""},
			{"versions": "", "encoding-type": "url", "key-marker": "k%", "version-id-marker": "null"},
		} {
			q := rec.urls[i].Query()
			for k, v := range want {
				if q.Get(k) != v || q.Has(k) != (v != "" || k == "versions") {
					t.Errorf("%s: request %d: query %s, want %s=%q", c.name, i, rec.urls[i].RawQuery, k, v)
				}
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
