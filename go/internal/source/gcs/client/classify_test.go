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

	"golang.org/x/oauth2"
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
		want      Class
		exhausted bool
	}{
		{"throttle", ClassThrottle, true},
		{"user-throttle", ClassThrottle, true},
		{"server", ClassServer, true},
		{"request-timeout", ClassTimeout, true},
		{"auth", ClassAuth, false},
		{"denied", ClassAccessDenied, false},
		{"missing", ClassNoSuchBucket, false},
		{"bad-request", ClassOther, false},
		{"parse", ClassParse, true},
		{"garbled", ClassParse, true},
		{"slow", ClassTimeout, true},
		{"cut", ClassTransport, true},
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
	if _, err := d.List(context.Background(), &ListInput{Bucket: "b"}); Classify(err) != ClassTransport {
		t.Errorf("refused connection: class %q (%v)", Classify(err), err)
	}
	// Credentials: a refused grant, and a token source that fails in transit.
	for _, x := range []struct {
		name      string
		err       error
		exhausted bool
	}{
		{"refused grant", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, ErrorCode: "invalid_grant"}, false},
		{"token endpoint 503", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 503}}, true},
		{"token endpoint unreachable", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
	} {
		e, _ := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", TokenSource: failingTokens{x.err}, MaxAttempts: 1})
		_, err := e.List(context.Background(), &ListInput{Bucket: "b"})
		var ex *ExhaustedError
		if Classify(err) != ClassAuth || errors.As(err, &ex) != x.exhausted {
			t.Errorf("%s: class %q exhausted=%v (%v)", x.name, Classify(err), errors.As(err, &ex), err)
		}
	}
	// The caller's cancellation, and contract breaks the engine reports.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.List(ctx, &ListInput{Bucket: "b", Prefix: "slow"}); Classify(err) != ClassCanceled {
		t.Errorf("canceled: class %q (%v)", Classify(err), err)
	}
	if got := Classify(fmt.Errorf("%w: out of order", ErrInvalidPage)); got != ClassInvalidPage {
		t.Errorf("invalid page: class %q", got)
	}
	if got := Classify(nil); got != "" {
		t.Errorf("nil: class %q", got)
	}
}
