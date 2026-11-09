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

// decoders are the SDK-backed client's decoders; every differential test
// runs over all of them.
var decoders = []struct {
	name string
	dec  Decoder
}{{"sdk", DecoderSDK}, {"walk", DecoderWalk}, {"scanner", DecoderScanner}}

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

// pair builds the hand-rolled client and the SDK-backed one with dec for
// the same config; hc, if set, is each one's supplied HTTP client (a copy
// per client).
func pair(t *testing.T, cfg client.Config, dec Decoder, hc *http.Client) (hand, sdk lister) {
	t.Helper()
	hcfg, scfg := cfg, cfg
	if hc != nil {
		h, s := *hc, *hc
		h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse } // the hand-rolled client keeps a supplied policy
		hcfg.HTTPClient, scfg.HTTPClient = &h, &s
	}
	h, err := client.New(context.Background(), hcfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(context.Background(), scfg, WithDecoder(dec))
	if err != nil {
		t.Fatal(err)
	}
	return h, s
}

// requests is a counter view: what the differential tests compare.
func requests(c client.Counters) client.Counters {
	return client.Counters{Attempts: c.Attempts, Retries: c.Retries, Throttles: c.Throttles, Timeouts: c.Timeouts, ParseFailures: c.ParseFailures}
}
