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
// TestURL), with the hand-rolled client's Route for each.
var routeCases = []struct {
	name   string
	cfg    client.Config
	bucket string
	hand   string // client.Client.Route
	sdk    string // Client.Route: the SDK's endpoint resolver
}{
	{"default endpoint", client.Config{Region: "eu-west-1"}, "bkt",
		"https://bkt.s3.eu-west-1.amazonaws.com/", "https://bkt.s3.eu-west-1.amazonaws.com"},
	{"dotted bucket", client.Config{Region: "us-east-1"}, "a.b",
		"https://s3.us-east-1.amazonaws.com/a.b", "https://s3.us-east-1.amazonaws.com/a.b"},
	{"path-style", client.Config{Region: "us-east-1", PathStyle: true}, "bkt",
		"https://s3.us-east-1.amazonaws.com/bkt", "https://s3.us-east-1.amazonaws.com/bkt"},
	{"custom IP endpoint", client.Config{Region: "us-east-1", Endpoint: "http://127.0.0.1:9000"}, "bkt",
		"http://127.0.0.1:9000/bkt", "http://127.0.0.1:9000/bkt"},
	{"custom endpoint with a path", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/"}, "bkt",
		"https://bkt.minio.example/s3/", "https://bkt.minio.example/s3/"},
	{"custom endpoint with a path, path-style", client.Config{Region: "us-east-1", Endpoint: "https://minio.example/s3/", PathStyle: true}, "bkt",
		"https://minio.example/s3/bkt", "https://minio.example/s3/bkt"},
	{"FIPS", client.Config{Region: "us-gov-west-1", FIPS: true}, "bkt",
		"https://bkt.s3-fips.us-gov-west-1.amazonaws.com/", "https://bkt.s3-fips.us-gov-west-1.amazonaws.com"},
	{"dual-stack", client.Config{Region: "eu-west-1", DualStack: true}, "bkt",
		"https://bkt.s3.dualstack.eu-west-1.amazonaws.com/", "https://bkt.s3.dualstack.eu-west-1.amazonaws.com"},
	{"FIPS and dual-stack", client.Config{Region: "us-east-1", FIPS: true, DualStack: true}, "bkt",
		"https://bkt.s3-fips.dualstack.us-east-1.amazonaws.com/", "https://bkt.s3-fips.dualstack.us-east-1.amazonaws.com"},
	{"cn-north-1", client.Config{Region: "cn-north-1"}, "bkt",
		"https://bkt.s3.cn-north-1.amazonaws.com.cn/", "https://bkt.s3.cn-north-1.amazonaws.com.cn"},
}

// TestRouteAgainstHandRolled records both clients' routes for the endpoint
// forms. They are not made equal here: a difference changes a journal's
// route identity, a decision for the resume contract. The table holds both
// as they are; the test fails when either changes.
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
		if h, s := hand.Route(c.bucket), sdk.Route(c.bucket); h != c.hand || s != c.sdk {
			t.Errorf("%s: hand-rolled %s (want %s), sdk %s (want %s)", c.name, h, c.hand, s, c.sdk)
		} else if h != s {
			t.Logf("%s: hand-rolled %s, sdk %s", c.name, h, s)
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
