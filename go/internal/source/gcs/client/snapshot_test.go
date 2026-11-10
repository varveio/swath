// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source/objhttp/objhttptest"
)

// wireSnapshot is the checked-in rendering of every request shape the
// client sends (wire, objhttptest.Snapshot).
const wireSnapshot = "testdata/wire.txt"

// TestWireSnapshot: each request the client sends, anonymous and with a
// fixed token, on Google's endpoint (with and without a billed project)
// and on a custom one: objects.list of the live objects and of every
// generation, each with no options and with every option (a continuation:
// offsets, delimiter, page size, page token), is method, intended
// authority, escaped path, every query parameter as sent, in order, every
// header value (duplicates kept) and the body's length, exactly as
// testdata/wire.txt has it. The generated client's X-Goog-Api-Client
// versions and the User-Agent's swath token and client version are
// replaced by their form. A difference is drift for review: the semantic
// tests (TestRequests, TestEndpoint, TestCredentialsAndHeaders) say
// whether it is wrong.
func TestWireSnapshot(t *testing.T) {
	rec := newRecorder(t)
	u, _ := url.Parse(rec.URL)
	ctx := context.Background()
	full := ListInput{Bucket: "bkt", Prefix: "p/a b+c%/é", Delimiter: "/", StartOffset: "p/a b+c%/é\x00",
		EndOffset: "p/e&=#?", PageToken: "tok/+=", MaxResults: 1000}
	var b strings.Builder
	for _, cl := range []struct {
		name  string
		cfg   Config
		token bool
	}{
		{"anonymous", Config{Anonymous: true}, false},
		{"token", Config{}, true},
		{"token, billed project", Config{UserProject: "billed-p"}, true},
		{"anonymous, custom endpoint", Config{Anonymous: true, Endpoint: "http://gcs.example/storage/v1/"}, false},
	} {
		to := &addressed{toServer: toServer{u}}
		cfg := cl.cfg
		cfg.HTTPClient = &http.Client{Transport: to}
		var c *Client
		var err error
		if cl.token {
			c, err = withTokens(cfg, tokens{})
		} else {
			c, err = New(ctx, cfg)
		}
		if err != nil {
			t.Fatal(err)
		}
		generations := full
		generations.Versions = true
		for _, call := range []struct {
			name string
			in   ListInput
		}{
			{"objects", ListInput{Bucket: "bkt"}},
			{"objects, every option", full},
			{"generations", ListInput{Bucket: "bkt", Versions: true}},
			{"generations, every option", generations},
		} {
			n := len(rec.reqs)
			if _, err := c.List(ctx, &call.in); err != nil {
				t.Fatalf("%s, %s: %v", cl.name, call.name, err)
			}
			if len(rec.reqs) != n+1 {
				t.Fatalf("%s, %s: %d requests", cl.name, call.name, len(rec.reqs)-n)
			}
			to.mu.Lock()
			origin := to.last.Scheme + "://" + to.last.Host
			to.mu.Unlock()
			fmt.Fprintf(&b, "== %s: %s\n%s", cl.name, call.name, wire(rec.reqs[n], origin, rec.bodies[n]))
		}
	}
	objhttptest.Snapshot(t, wireSnapshot, b.String())
}

var clientVersion = regexp.MustCompile(`\b(gl-go|gdcl|google-api-go-client)/\S+`)

// wire renders one request the test server got (objhttptest.Request) on
// origin, where it was addressed. The X-Goog-Api-Client versions (Go's and
// the generated client's) and the User-Agent's swath token and the
// generated client's version are replaced by their form; anything else is
// as sent.
func wire(req *http.Request, origin string, body int) string {
	return objhttptest.Request(req, origin, body, func(k, v string) (string, bool) {
		switch k {
		case "X-Goog-Api-Client":
			v = clientVersion.ReplaceAllString(v, "${1}/<version>")
		case "User-Agent":
			v = strings.Replace(v, buildinfo.UserAgent(), "<swath>", 1)
			v = clientVersion.ReplaceAllString(v, "${1}/<version>")
		}
		return v, true
	})
}
