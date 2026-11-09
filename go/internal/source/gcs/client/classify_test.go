// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

type failingTokens struct{ err error }

func (f failingTokens) Token() (*oauth2.Token, error) { return nil, f.err }

// TestClassify: every failure a listing request can end in has its class,
// through the client as a run meets it (one attempt, so nothing is
// retried away); transient classes come back wrapped in ExhaustedError.
func TestClassify(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answer := func(status int, reason string) {
			w.WriteHeader(status)
			fmt.Fprintf(w, `{"error":{"code":%d,"message":"m","errors":[{"reason":%q}]}}`, status, reason)
		}
		switch r.URL.Query().Get("prefix") {
		case "throttle":
			answer(429, "rateLimitExceeded")
		case "user-throttle":
			answer(403, "userRateLimitExceeded")
		case "server":
			answer(503, "backendError")
		case "request-timeout":
			answer(408, "")
		case "auth":
			answer(401, "required")
		case "denied":
			answer(403, "forbidden")
		case "missing":
			answer(404, "notFound")
		case "bad-request":
			answer(400, "invalid")
		case "parse":
			fmt.Fprint(w, `{"items":[{"name":"a","size":"x"}]}`)
		case "garbled":
			fmt.Fprint(w, `{"items":[`)
		case "slow":
			select {
			case <-time.After(5 * time.Second):
			case <-r.Context().Done():
			}
		case "redirect":
			http.Redirect(w, r, "/storage/v1/b/other/o", http.StatusTemporaryRedirect)
		case "cut":
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			conn.Close()
		}
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 1,
		RequestTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []struct {
		prefix    string
		want      objhttp.Class
		exhausted bool
	}{
		{"throttle", objhttp.ClassThrottle, true},
		{"user-throttle", objhttp.ClassThrottle, true},
		{"server", objhttp.ClassServer, true},
		{"request-timeout", objhttp.ClassTimeout, true},
		{"auth", objhttp.ClassAuth, false},
		{"denied", objhttp.ClassAccessDenied, false},
		{"missing", objhttp.ClassNoSuchBucket, false},
		{"bad-request", objhttp.ClassOther, false},
		{"parse", objhttp.ClassParse, true},
		{"garbled", objhttp.ClassParse, true},
		{"slow", objhttp.ClassTimeout, true},
		{"cut", objhttp.ClassTransport, true},
		{"redirect", objhttp.ClassRedirect, false},
	} {
		_, err := c.List(context.Background(), &ListInput{Bucket: "b", Prefix: x.prefix})
		var ex *ExhaustedError
		if got := Classify(err); got != x.want || errors.As(err, &ex) != x.exhausted {
			t.Errorf("%s: class %q exhausted=%v (%v), want %q exhausted=%v", x.prefix, got, errors.As(err, &ex), err, x.want, x.exhausted)
		}
	}
	// Transport: nothing listens.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	d, _ := New(context.Background(), Config{Endpoint: "http://" + addr + "/storage/v1/", Anonymous: true, MaxAttempts: 1})
	if _, err := d.List(context.Background(), &ListInput{Bucket: "b"}); Classify(err) != objhttp.ClassTransport {
		t.Errorf("refused connection: class %q (%v)", Classify(err), err)
	}
	// Credentials: a refused grant, and a token source that fails in transit.
	for _, x := range []struct {
		name      string
		err       error
		exhausted bool
		class     objhttp.Class // when not the default for exhausted
	}{
		{"refused grant", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, ErrorCode: "invalid_grant"}, false, ""},
		{"token endpoint 503", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 503}}, true, ""},
		{"token endpoint unreachable", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true, ""},
		{"metadata server 503", &metadata.Error{Code: 503, Message: "unavailable"}, true, ""},
		{"metadata server 429", fmt.Errorf("compute: %w", &metadata.Error{Code: 429}), true, ""},
		{"metadata server 404", &metadata.Error{Code: 404, Message: "no service account"}, false, ""},
		{"token exchange timeout", fmt.Errorf("oauth2: %w", context.DeadlineExceeded), true, objhttp.ClassTimeout},
	} {
		e, _ := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", TokenSource: failingTokens{x.err}, MaxAttempts: 1})
		_, err := e.List(context.Background(), &ListInput{Bucket: "b"})
		var ex *ExhaustedError
		want := objhttp.ClassAuth // a refusal stops the run; an outage of the token endpoint is transient
		if x.exhausted {
			want = objhttp.ClassTransport
		}
		if x.class != "" {
			want = x.class
		}
		var ce *CredentialError
		if Classify(err) != want || errors.As(err, &ex) != x.exhausted || !errors.As(err, &ce) {
			t.Errorf("%s: class %q exhausted=%v (%v)", x.name, Classify(err), errors.As(err, &ex), err)
		}
	}
	// The caller's cancellation, and contract breaks the engine reports.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.List(ctx, &ListInput{Bucket: "b", Prefix: "slow"}); Classify(err) != objhttp.ClassCanceled {
		t.Errorf("canceled: class %q (%v)", Classify(err), err)
	}
	if got := Classify(fmt.Errorf("%w: out of order", ErrInvalidPage)); got != objhttp.ClassInvalidPage {
		t.Errorf("invalid page: class %q", got)
	}
	if got := Classify(nil); got != "" {
		t.Errorf("nil: class %q", got)
	}
}

// TestFailureSignals: an attempt's failure is normalized once; its
// admission signal follows its class, so a 408 (class timeout) is a
// timeout signal to the limiter, a 429 a throttle, a 503 neither, and all
// three are retried. Before, the 408 carried no timeout signal.
func TestFailureSignals(t *testing.T) {
	for _, c := range []struct {
		status            int
		class             objhttp.Class
		throttle, timeout bool
	}{
		{408, objhttp.ClassTimeout, false, true},
		{429, objhttp.ClassThrottle, true, false},
		{503, objhttp.ClassServer, false, false},
	} {
		f := objhttp.Normalize(&Error{Status: c.status}, Answer)
		if f.Class != c.class || !f.Retry || f.Throttle != c.throttle || f.Timeout != c.timeout {
			t.Fatalf("%d: %+v", c.status, f)
		}
	}
}
