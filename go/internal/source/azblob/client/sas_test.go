// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// testSAS is a SAS whose signature is easy to find in a message.
const testSAS = "sv=2026-10-06&ss=b&srt=co&sp=rl&se=2026-12-01T00%3A00%3A00Z&sig=TopSecretSig%2Bpart%3D"

// TestSASOnEveryRequest: the SAS (with or without '?') is appended to the
// listing and account-information queries as given, and nothing else
// authenticates the request.
func TestSASOnEveryRequest(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		if r.Header.Get(headerAuthorization) != "" || r.Header.Get(headerDate) != "" {
			t.Errorf("headers %v", r.Header)
		}
		if r.URL.Query().Get("comp") == "properties" {
			w.Header().Set(headerHNSEnabled, "false")
			return
		}
		fmt.Fprint(w, page(""))
	}))
	defer srv.Close()
	for _, sas := range []string{testSAS, "?" + testSAS} {
		queries = nil
		// The SAS precedes a shared key and a token credential.
		c := newTestClient(t, Config{Endpoint: srv.URL, SAS: sas, SharedKey: "a2V5", Credential: failingCredential{}})
		if _, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: "p"}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.AccountInfo(context.Background(), "c"); err != nil {
			t.Fatal(err)
		}
		for _, q := range queries {
			if !strings.HasSuffix(q, "&"+testSAS) {
				t.Errorf("query %q lacks the SAS", q)
			}
		}
		if src, anon := c.Credentials(); src != "shared access signature" || anon || strings.Contains(c.Route("c"), "sig") {
			t.Errorf("credentials %q anonymous=%v route %s", src, anon, c.Route("c"))
		}
	}
}

// TestSASRedacted: the SAS appears in no error: not a transport error's
// URL, a 400's query echo, a redirect target, nor a message that quotes
// it.
func TestSASRedacted(t *testing.T) {
	sig, _ := url.QueryUnescape("TopSecretSig%2Bpart%3D")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("prefix") {
		case "400":
			w.Header().Set(headerErrorCode, "InvalidQueryParameterValue")
			w.WriteHeader(400)
			var msg strings.Builder
			xml.EscapeText(&msg, []byte("bad sig "+sig+" in "+r.URL.RawQuery))
			fmt.Fprintf(w, "\xef\xbb\xbf<Error><Code>InvalidQueryParameterValue</Code><Message>%s</Message></Error>", msg.String())
		case "403":
			w.Header().Set(headerErrorCode, codeAuthFailed)
			w.WriteHeader(403)
			fmt.Fprintf(w, "\xef\xbb\xbf<Error><Code>AuthenticationFailed</Code><Message>Signature %s did not match</Message></Error>", url.QueryEscape(sig))
		case "redirect":
			http.Redirect(w, r, "http://elsewhere.example/?"+r.URL.RawQuery, http.StatusMovedPermanently)
		case "bad-location":
			// net/http would quote a Location it cannot parse in its error
			w.Header().Set("Location", "https://elsewhere/%ZZ?"+r.URL.RawQuery)
			w.WriteHeader(http.StatusFound)
		case "slow":
			select {
			case <-time.After(5 * time.Second):
			case <-r.Context().Done():
			}
		}
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, SAS: testSAS, MaxAttempts: 1, RequestTimeout: 100 * time.Millisecond})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	down := newTestClient(t, Config{Endpoint: "http://" + addr, SAS: testSAS, MaxAttempts: 1})
	for _, x := range []struct {
		c      *Client
		prefix string
	}{{c, "400"}, {c, "403"}, {c, "redirect"}, {c, "bad-location"}, {c, "slow"}, {down, "refused"}} {
		_, err := x.c.List(context.Background(), &ListInput{Container: "c", Prefix: x.prefix})
		if err == nil {
			t.Fatalf("%s: no error", x.prefix)
		}
		if msg := err.Error(); strings.Contains(msg, "TopSecretSig") || strings.Contains(msg, "se=2026") || strings.Contains(msg, "elsewhere") {
			t.Errorf("%s: the SAS or a redirect target leaks: %s", x.prefix, msg)
		}
		if strings.Contains(x.prefix, "redirect") || x.prefix == "bad-location" {
			if Classify(err) != objhttp.ClassRedirect {
				t.Errorf("%s: class %q (%v)", x.prefix, Classify(err), err)
			}
		}
		if _, err := x.c.AccountInfo(context.Background(), x.prefix); err != nil && strings.Contains(err.Error(), "TopSecretSig") {
			t.Errorf("%s: account information leaks the SAS: %v", x.prefix, err)
		}
	}
	// The 400's message was read (the fixture is well-formed XML) and
	// scrubbed, not dropped.
	if _, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: "400"}); !strings.Contains(err.Error(), "prefix=400") ||
		!strings.Contains(err.Error(), "bad sig REDACTED in comp=list") {
		t.Errorf("a 400 names its query and its scrubbed message: %v", err)
	}
	// Recursive redaction: a SAS quoted deep inside a wrapped error.
	u, _ := url.Parse(srv.URL + "/acct/c?comp=list")
	inner := &url.Error{Op: "Get", URL: srv.URL + "?" + testSAS, Err: fmt.Errorf("wrapped: %w", &url.Error{Op: "Get", URL: "x?" + testSAS, Err: fmt.Errorf("location %q", "https://e/?sig="+sig)})}
	if msg := c.redact(fmt.Errorf("outer: %w", inner), u).Error(); strings.Contains(msg, "TopSecretSig") || strings.Contains(msg, "se=2026") {
		t.Errorf("nested errors leak: %s", msg)
	}
	if _, err := New(context.Background(), Config{Account: "acct", SAS: "%zz&sig=TopSecretSig"}); err == nil || strings.Contains(err.Error(), "TopSecretSig") {
		t.Errorf("malformed SAS: %v", err)
	}
}
