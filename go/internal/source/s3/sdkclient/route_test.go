// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// recorder answers every request with okPage, without a network, and
// keeps the URLs.
type recorder struct {
	mu   sync.Mutex
	urls []string
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.urls = append(r.urls, req.URL.String())
	r.mu.Unlock()
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(okPage)), Request: req}, nil
}

// routeCases are the endpoint forms the provider supports (package client's
// TestURL), with the route of each.
var routeCases = []struct {
	name   string
	cfg    client.Config
	bucket string
	route  string // both clients' Route
}{
	{"default endpoint", client.Config{Region: "eu-west-1"}, "bkt",
		"https://bkt.s3.eu-west-1.amazonaws.com/"},
	{"dotted bucket", client.Config{Region: "us-east-1"}, "a.b",
		"https://s3.us-east-1.amazonaws.com/a.b"},
	{"path-style", client.Config{Region: "us-east-1", PathStyle: true}, "bkt",
		"https://s3.us-east-1.amazonaws.com/bkt"},
	{"custom IP endpoint", client.Config{Region: "us-east-1", Endpoint: "http://127.0.0.1:9000"}, "bkt",
		"http://127.0.0.1:9000/bkt"},
	{"custom endpoint with a path", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/"}, "bkt",
		"https://bkt.minio.example/s3/"},
	{"custom endpoint with a path, path-style", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/", PathStyle: true}, "bkt",
		"https://minio.example/s3/bkt"},
	{"FIPS", client.Config{Region: "us-gov-west-1", FIPS: true}, "bkt",
		"https://bkt.s3-fips.us-gov-west-1.amazonaws.com/"},
	{"dual-stack", client.Config{Region: "eu-west-1", DualStack: true}, "bkt",
		"https://bkt.s3.dualstack.eu-west-1.amazonaws.com/"},
	{"FIPS and dual-stack", client.Config{Region: "us-east-1", FIPS: true, DualStack: true}, "bkt",
		"https://bkt.s3-fips.dualstack.us-east-1.amazonaws.com/"},
	{"cn-north-1", client.Config{Region: "cn-north-1"}, "bkt",
		"https://bkt.s3.cn-north-1.amazonaws.com.cn/"},
}

// TestRouteAgainstHandRolled: both clients name the same route for every
// endpoint form (a journal's route identity); the table holds both, so a
// change in either fails here.
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
		if h, s := hand.Route(c.bucket), sdk.Route(c.bucket); h != c.route || s != c.route {
			t.Errorf("%s: hand-rolled %s, sdk %s, want %s", c.name, h, s, c.route)
		}
	}
}

// TestFirstRequestFollowsRoute: the first request's URL starts with Route,
// for every endpoint form and for both listings.
func TestFirstRequestFollowsRoute(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range routeCases {
		for _, versions := range []bool{false, true} {
			rec := &recorder{}
			c.cfg.Anonymous, c.cfg.HTTPClient, c.cfg.MaxAttempts = true, &http.Client{Transport: rec}, 1
			sdk, err := New(context.Background(), c.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if versions {
				_, err = sdk.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: c.bucket})
			} else {
				_, err = sdk.ListObjectsV2(context.Background(), &client.ListInput{Bucket: c.bucket, Prefix: "p/"})
			}
			if versions && err == nil || !versions && err != nil {
				// okPage is no ListVersionsResult: the versions call fails
				// after its request, which is what is checked here.
				t.Fatalf("%s: %v", c.name, err)
			}
			if r := sdk.Route(c.bucket); len(rec.urls) == 0 || !strings.HasPrefix(rec.urls[0], r) {
				t.Fatalf("%s: first request %v, route %s", c.name, rec.urls, r)
			}
		}
	}
}

// TestFIPSRegionAliases: a FIPS region alias is its region with FIPS on,
// for Route as for the requests (the SDK resolves it inside each
// operation), and the signing region is the region.
func TestFIPSRegionAliases(t *testing.T) {
	s3test.NoCredentials(t)
	for _, region := range []string{"fips-us-east-1", "us-east-1-fips"} {
		rec := &recorder{}
		c, err := New(context.Background(), client.Config{Region: region, Anonymous: true, MaxAttempts: 1, HTTPClient: &http.Client{Transport: rec}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ListObjectsV2(context.Background(), &client.ListInput{Bucket: "bkt"}); err != nil {
			t.Fatal(err)
		}
		r := c.Route("bkt")
		if r != "https://bkt.s3-fips.us-east-1.amazonaws.com/" || c.Region() != "us-east-1" || !strings.HasPrefix(rec.urls[0], r) {
			t.Fatalf("%s: route %s, region %s, first request %s", region, r, c.Region(), rec.urls[0])
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
