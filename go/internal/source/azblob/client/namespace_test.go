// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// toFake sends every request to the fake server, keeping the host it was
// addressed to in X-Original-Host, so a client on the default endpoint
// takes its production path.
type toFake struct{ to *url.URL }

func (t toFake) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("X-Original-Host", req.URL.Host)
	req.URL.Scheme, req.URL.Host, req.Host = t.to.Scheme, t.to.Host, t.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestNamespaceInfo: Filesystem Get Properties is a HEAD of the
// container on the account's DFS host with resource=filesystem (the SAS
// appended); x-ms-namespace-enabled true or false is the answer, a
// missing header or a refusal is an error, and an endpoint that is not an
// account's blob host has no DFS host to ask.
func TestNamespaceInfo(t *testing.T) {
	var mu sync.Mutex
	var seen []*http.Request
	answer := "false"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r)
		a := answer
		mu.Unlock()
		switch a {
		case "401":
			w.Header().Set("x-ms-error-code", codeNoAuthentication)
			w.WriteHeader(401)
		case "":
		default:
			w.Header().Set("x-ms-namespace-enabled", a)
		}
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	ctx := context.Background()
	for _, sas := range []string{"", "sv=1&sig=S"} {
		c, err := New(ctx, Config{Account: "acct", Anonymous: sas == "", SAS: sas, MaxAttempts: 1, HTTPClient: &http.Client{Transport: toFake{u}}})
		if err != nil {
			t.Fatal(err)
		}
		for a, want := range map[string]bool{"false": false, "True": true} {
			mu.Lock()
			answer, seen = a, nil
			mu.Unlock()
			got, err := c.NamespaceInfo(ctx, "cont")
			if err != nil || got != want {
				t.Fatalf("%s: %v %v", a, got, err)
			}
			r := seen[0]
			q := r.URL.Query()
			if r.Method != http.MethodHead || r.Header.Get("X-Original-Host") != "acct.dfs.core.windows.net" || r.URL.Path != "/cont" ||
				q.Get("resource") != "filesystem" || q.Get("sig") != map[string]string{"": "", "sv=1&sig=S": "S"}[sas] ||
				r.Header.Get("x-ms-version") != Version {
				t.Fatalf("request %s %s%s?%s", r.Method, r.Header.Get("X-Original-Host"), r.URL.Path, r.URL.RawQuery)
			}
		}
	}
	// Shared Key: not asked (the request is not signed), no request sent.
	k, err := New(ctx, Config{Account: "acct", SharedKey: testKey, MaxAttempts: 1, HTTPClient: &http.Client{Transport: toFake{u}}})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	n := len(seen)
	mu.Unlock()
	if _, err := k.NamespaceInfo(ctx, "cont"); !errors.Is(err, ErrNoDFSEndpoint) || len(seen) != n {
		t.Fatalf("Shared Key: %v, %d requests", err, len(seen)-n)
	}
	c, _ := New(ctx, Config{Account: "acct", Anonymous: true, MaxAttempts: 1, HTTPClient: &http.Client{Transport: toFake{u}}})
	mu.Lock()
	answer = ""
	mu.Unlock()
	if _, err := c.NamespaceInfo(ctx, "cont"); !errors.Is(err, ErrNamespaceUnknown) {
		t.Fatalf("missing header: %v", err)
	}
	mu.Lock()
	answer = "401"
	mu.Unlock()
	if _, err := c.NamespaceInfo(ctx, "cont"); Classify(err) != objhttp.ClassAuth {
		t.Fatalf("401: %v (%s)", err, Classify(err))
	}
	mu.Lock()
	answer = "false"
	mu.Unlock()
	for ep, dfs := range map[string]bool{
		srv.URL: false,
		"https://acct.blob.core.windows.net/proxy": false, // a path: a proxy, not the account's host
		"https://acct.blob.core.windows.net:8443":  false,
		"https://other.blob.core.windows.net":      false, // another account's host
		"http://acct.blob.core.windows.net":        false,
		"https://ACCT.blob.core.windows.net/":      true,
	} {
		other, err := New(ctx, Config{Account: "acct", Endpoint: ep, Anonymous: true, MaxAttempts: 1, HTTPClient: &http.Client{Transport: toFake{u}}})
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		_, err = other.NamespaceInfo(ctx, "cont")
		mu.Lock()
		sent := seen[n:]
		mu.Unlock()
		if dfs != (err == nil) || !dfs && (!errors.Is(err, ErrNoDFSEndpoint) || len(sent) != 0) {
			t.Fatalf("endpoint %s: %v after %d requests", ep, err, len(sent))
		}
		if dfs && (sent[0].Header.Get("X-Original-Host") != "acct.dfs.core.windows.net" || sent[0].URL.Path != "/cont") {
			t.Fatalf("endpoint %s: sent to %s%s", ep, sent[0].Header.Get("X-Original-Host"), sent[0].URL.Path)
		}
	}
}
