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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/objhttp/objhttptest"
)

// wireSnapshot is the checked-in rendering of every request shape the
// client sends (wire, objhttptest.Snapshot).
const wireSnapshot = "testdata/wire.txt"

// TestWireSnapshot: each request the client sends, under each
// authentication form (anonymous, SAS, Shared Key, Entra ID), on the
// default endpoint: List Blobs flat and with a delimiter, with no options
// and with all of them (a continuation: marker and startFrom), Get Account
// Information, and the Data Lake HEAD (not asked under Shared Key) is
// method, intended authority, escaped path, every query parameter as sent,
// in order, every header value (duplicates kept) and the body's length,
// exactly as testdata/wire.txt has it. Shared Key signatures are verified
// before the rendering replaces them; dates and the telemetry's version and
// platform text are replaced too. A difference is SDK drift for review: the
// semantic tests (TestServiceVersionPinned, TestStartFromSent) say whether
// it is wrong.
func TestWireSnapshot(t *testing.T) {
	var cp reqLog
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set(headerNamespaceEnabled, "false")
		case r.URL.Query().Get("comp") == "list":
			fmt.Fprint(w, page(""))
		default:
			w.Header().Set(headerHNSEnabled, "false")
		}
	})
	u, _ := url.Parse(srv.URL)
	hc := &http.Client{Transport: toFake{u}}
	full := ListInput{Container: "c", Prefix: "a b+c/\u00e9", StartFrom: "a b+c/\u00e9/x", Marker: "m", MaxResults: 10}
	hier := full
	hier.Delimiter = "/"
	ctx := context.Background()
	var b strings.Builder
	for _, auth := range []struct {
		name  string
		cfg   Config
		entra azcore.TokenCredential
	}{
		{"anonymous", Config{Anonymous: true}, nil},
		{"SAS", Config{SAS: testSAS}, nil},
		{"Shared Key", Config{SharedKey: testKey}, nil},
		{"Entra ID", Config{}, fakeToken{tok: "tok"}},
	} {
		cfg := auth.cfg
		cfg.Account, cfg.HTTPClient, cfg.Retry = "acct", hc, objhttp.Retry{MaxAttempts: 1}
		var c *Client
		var err error
		if auth.entra != nil {
			c, err = newClient(ctx, cfg, withEntra(auth.entra))
		} else {
			c, err = New(ctx, cfg)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range []struct {
			name string
			do   func() error
		}{
			{"flat", func() error { _, err := c.List(ctx, &ListInput{Container: "c"}); return err }},
			{"flat, every option", func() error { _, err := c.List(ctx, &full); return err }},
			{"hierarchy", func() error { _, err := c.List(ctx, &ListInput{Container: "c", Delimiter: "/"}); return err }},
			{"hierarchy, every option", func() error { _, err := c.List(ctx, &hier); return err }},
			{"account information", func() error { _, err := c.AccountInfo(ctx, "c"); return err }},
			{"filesystem properties", func() error { _, err := c.NamespaceInfo(ctx, "c"); return err }},
		} {
			n := len(cp.reqs)
			if err := call.do(); err != nil {
				if cfg.SharedKey != "" && call.name == "filesystem properties" && len(cp.reqs) == n {
					continue // not asked under Shared Key (TestNamespaceInfo)
				}
				t.Fatalf("%s, %s: %v", auth.name, call.name, err)
			}
			if len(cp.reqs) != n+1 {
				t.Fatalf("%s, %s: %d requests", auth.name, call.name, len(cp.reqs)-n)
			}
			req := cp.reqs[n]
			if cfg.SharedKey != "" {
				verifySharedKey(t, req, "acct", cfg.SharedKey)
			}
			fmt.Fprintf(&b, "== %s: %s\n%s", auth.name, call.name, wire(t, req, cp.bodies[n]))
		}
	}
	objhttptest.Snapshot(t, wireSnapshot, b.String())
}

var (
	sdkToken = regexp.MustCompile(`azsdk-go-azblob/v\S+ \([^)]*\)`)
	sigForm  = regexp.MustCompile(`^SharedKey acct:[A-Za-z0-9+/]{43}=$`)
)

// wire renders one request toFake delivered (objhttptest.Request), on the
// authority it was addressed to, without toFake's X-Original-Host.
// x-ms-date (an HTTP date), a Shared Key signature (verified by the caller)
// and the SDK's telemetry token's version and platform are replaced by
// their form; anything else is as sent.
func wire(t *testing.T, req *http.Request, body int) string {
	t.Helper()
	return objhttptest.Request(req, "https://"+req.Header.Get("X-Original-Host"), body, func(k, v string) (string, bool) {
		switch k {
		case "X-Original-Host":
			return "", false
		case "X-Ms-Date":
			if _, err := time.Parse(http.TimeFormat, v); err != nil {
				t.Errorf("x-ms-date %q: %v", v, err)
			}
			v = "<date>"
		case "Authorization":
			if strings.HasPrefix(v, "SharedKey ") {
				if !sigForm.MatchString(v) {
					t.Errorf("Authorization %q", v)
				}
				v = "SharedKey acct:<signature>"
			}
		case "User-Agent":
			v = strings.Replace(v, buildinfo.UserAgent(), "<swath>", 1)
			v = sdkToken.ReplaceAllString(v, "azsdk-go-azblob/<version> (<platform>)")
		}
		return v, true
	})
}
