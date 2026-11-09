// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/s3/client"
)

// decoders are the SDK-backed client's paths: the scanner (production) and
// the SDK's deserializer for current objects (measure.go; dec true). Every
// differential test runs over both.
var decoders = []struct {
	name string
	dec  bool
}{{"scanner", false}, {"sdk", true}}

// production is the production path alone, for the tests of what a 200
// body must be (the measurement path checks no envelope: measure.go).
var production = decoders[:1]

// newSDK is New with the decoder path chosen (not the environment's).
func newSDK(ctx context.Context, cfg client.Config, sdkDecode bool) (*Client, error) {
	c, err := New(ctx, cfg)
	if c != nil {
		c.sdkDecode = sdkDecode
	}
	return c, err
}

// lister is what the differential tests compare: the hand-rolled client
// and the SDK-backed one.
type lister interface {
	ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error)
	ListObjectVersions(ctx context.Context, in *client.VersionsInput) (*client.VersionPage, error)
	Counters() client.Counters
}

// testConfig is an anonymous path-style config for endpoint with tiny
// backoffs.
func testConfig(endpoint string) client.Config {
	return client.Config{Endpoint: endpoint, PathStyle: true, Anonymous: true, Region: "us-east-1",
		BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
}

// pair builds the hand-rolled client and the SDK-backed one (dec: newSDK) for
// the same config; hc, if set, is each one's supplied HTTP client (a copy
// per client).
func pair(t *testing.T, cfg client.Config, dec bool, hc *http.Client) (hand, sdk lister) {
	t.Helper()
	hcfg, scfg := cfg, cfg
	if hc != nil {
		h, s := *hc, *hc
		hcfg.HTTPClient, scfg.HTTPClient = &h, &s
	}
	h, err := client.New(context.Background(), hcfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newSDK(context.Background(), scfg, dec)
	if err != nil {
		t.Fatal(err)
	}
	return h, s
}

// requests is a counter view: what the differential tests compare.
func requests(c client.Counters) client.Counters {
	return client.Counters{Attempts: c.Attempts, Retries: c.Retries, Throttles: c.Throttles, Timeouts: c.Timeouts, ParseFailures: c.ParseFailures}
}
