// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source/s3/client"
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
	c, err := New(context.Background(), client.Config{Region: "us-east-1", Anonymous: true,
		HTTPClient: &http.Client{Transport: rec}, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []*client.ListInput{{Bucket: "b"}, {Bucket: "b", ContinuationToken: "next"}} {
		if _, err := c.ListObjectsV2(context.Background(), in); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"}); err != nil {
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

// routeCases are the endpoint forms the provider supports (package client's
// TestURL), with the route of each: both clients', but for hand, the
// hand-rolled client's where it differs (a finding, not hidden here).
var routeCases = []struct {
	name   string
	cfg    client.Config
	bucket string
	route  string
	hand   string
}{
	{"default endpoint", client.Config{Region: "eu-west-1"}, "bkt", "https://bkt.s3.eu-west-1.amazonaws.com/", ""},
	{"dotted bucket", client.Config{Region: "us-east-1"}, "a.b", "https://s3.us-east-1.amazonaws.com/a.b", ""},
	{"path-style", client.Config{Region: "us-east-1", PathStyle: true}, "bkt", "https://s3.us-east-1.amazonaws.com/bkt", ""},
	{"custom IP endpoint", client.Config{Region: "us-east-1", Endpoint: "http://127.0.0.1:9000"}, "bkt", "http://127.0.0.1:9000/bkt", ""},
	{"custom IPv6 endpoint", client.Config{Region: "us-east-1", Endpoint: "http://[::1]:9000"}, "bkt", "http://[::1]:9000/bkt", ""},
	{"custom endpoint with a path", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/"}, "bkt",
		"https://bkt.minio.example/s3/", ""},
	{"custom endpoint with a path, path-style", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/", PathStyle: true}, "bkt",
		"https://minio.example/s3/bkt", ""},
	{"custom endpoint with an escaped path", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/a%20b/", PathStyle: true}, "bkt",
		"https://minio.example/a%20b/bkt", ""},
	{"custom endpoint with dot segments", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/x/../y/", PathStyle: true}, "bkt",
		"https://minio.example/x/../y/bkt", ""},
	{"FIPS", client.Config{Region: "us-gov-west-1", FIPS: true}, "bkt", "https://bkt.s3-fips.us-gov-west-1.amazonaws.com/", ""},
	{"dual-stack", client.Config{Region: "eu-west-1", DualStack: true}, "bkt", "https://bkt.s3.dualstack.eu-west-1.amazonaws.com/", ""},
	{"FIPS and dual-stack", client.Config{Region: "us-east-1", FIPS: true, DualStack: true}, "bkt",
		"https://bkt.s3-fips.dualstack.us-east-1.amazonaws.com/", ""},
	{"cn-north-1", client.Config{Region: "cn-north-1"}, "bkt", "https://bkt.s3.cn-north-1.amazonaws.com.cn/", ""},
	{"FIPS alias fips-us-east-1", client.Config{Region: "fips-us-east-1"}, "bkt",
		"https://bkt.s3-fips.us-east-1.amazonaws.com/", "https://bkt.s3.fips-us-east-1.amazonaws.com/"},
	{"FIPS alias us-east-1-fips", client.Config{Region: "us-east-1-fips"}, "bkt",
		"https://bkt.s3-fips.us-east-1.amazonaws.com/", "https://bkt.s3.us-east-1-fips.amazonaws.com/"},
}

// TestRouteAgainstHandRolled: both clients name the same route for every
// endpoint form (a journal's route identity) but the FIPS region aliases,
// which the hand-rolled client takes for regions (a finding); the table
// holds both, so a change in either fails here.
func TestRouteAgainstHandRolled(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range routeCases {
		c.cfg.Anonymous = true
		hand, err := client.New(context.Background(), c.cfg)
		if err != nil {
			t.Fatal(err)
		}
		sdk, err := New(context.Background(), c.cfg)
		if err != nil {
			t.Fatal(err)
		}
		want := c.hand
		if want == "" {
			want = c.route
		}
		if h, s := hand.Route(c.bucket), sdk.Route(c.bucket); h != want || s != c.route {
			t.Errorf("%s: hand-rolled %s (want %s), sdk %s (want %s)", c.name, h, want, s, c.route)
		}
	}
}

// TestRequestsFollowRoute: every request a client sends goes to its Route
// exactly (scheme, host and escaped path; the query aside): the first
// page, a continuation, a retried attempt and a versions page, for every
// endpoint form, through both clients. The requests are recorded under
// the Guard.
func TestRequestsFollowRoute(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range routeCases {
		for _, who := range []string{"hand-rolled", "sdk"} {
			rec := &recorder{fail: 1}
			cfg := c.cfg
			cfg.Anonymous, cfg.HTTPClient, cfg.MaxAttempts = true, &http.Client{Transport: rec}, 2
			var cl interface {
				lister
				Route(string) string
			}
			var err error
			if who == "sdk" {
				cl, err = New(context.Background(), cfg)
			} else {
				cl, err = client.New(context.Background(), cfg)
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			for _, call := range []func() error{
				func() error { // a 503, then the first page: the retried attempt
					_, err := cl.ListObjectsV2(ctx, &client.ListInput{Bucket: c.bucket, Prefix: "p/", StartAfter: "p/a b"})
					return err
				},
				func() error {
					_, err := cl.ListObjectsV2(ctx, &client.ListInput{Bucket: c.bucket, ContinuationToken: "1/tok+=="})
					return err
				},
				func() error {
					_, err := cl.ListObjectVersions(ctx, &client.VersionsInput{Bucket: c.bucket, KeyMarker: "k%", VersionIDMarker: "null"})
					return err
				},
			} {
				if err := call(); err != nil {
					t.Fatalf("%s/%s: %v", c.name, who, err)
				}
			}
			r := cl.Route(c.bucket)
			if len(rec.urls) != 4 {
				t.Fatalf("%s/%s: %d requests", c.name, who, len(rec.urls))
			}
			for i, u := range rec.urls {
				if routeOf(u) != r {
					t.Errorf("%s/%s: request %d to %s, route %s", c.name, who, i, u, r)
				}
			}
		}
	}
}

// TestCustomEndpointIgnoresEndpointFlags: with a custom endpoint, FIPS and
// dual-stack are not used (they select among AWS endpoints), as by the
// hand-rolled client: an S3-compatible store lists, path-style, with
// either flag, on the same route.
func TestCustomEndpointIgnoresEndpointFlags(t *testing.T) {
	s3test.NoCredentials(t)
	url := s3test.Bucket(t, []string{"a", "b/c"})
	for _, flags := range []client.Config{{FIPS: true}, {DualStack: true}, {FIPS: true, DualStack: true}} {
		cfg := testConfig(url)
		cfg.FIPS, cfg.DualStack = flags.FIPS, flags.DualStack
		hand, sdk := pair(t, cfg, false, nil)
		pg, err := sdk.ListObjectsV2(context.Background(), &client.ListInput{Bucket: s3test.Name})
		if err != nil || len(pg.Contents) != 2 {
			t.Fatalf("%+v: %v %+v", flags, err, pg)
		}
		if h, s := hand.(*client.Client).Route(s3test.Name), sdk.(*Client).Route(s3test.Name); h != s || s != url+"/"+s3test.Name {
			t.Fatalf("%+v: hand-rolled %s, sdk %s", flags, h, s)
		}
	}
}
