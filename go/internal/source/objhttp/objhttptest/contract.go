// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package objhttptest is the retry and classification contract every
// object-store client on objhttp passes: each client's tests run Contract
// with its own wire answers.
package objhttptest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// Client is one client under test, built for an endpoint.
type Client interface {
	List(ctx context.Context) error // one listing request, retries included
	Counters() objhttp.Counts
}

// Wire writes the client's service answers.
type Wire struct {
	OK       func(w http.ResponseWriter) // a valid (empty, final) page
	Throttle func(w http.ResponseWriter) // the service's slow-down answer
	Server   func(w http.ResponseWriter) // a 5xx
	Denied   func(w http.ResponseWriter) // access denied (403)
	Garbled  func(w http.ResponseWriter) // a 200 whose body does not parse
}

// Settings are what the contract builds a client with.
type Settings struct {
	Endpoint string // the test server's base URL
	Retry    objhttp.Retry
	Timeout  time.Duration // per attempt
}

type answer int

const (
	ok answer = iota
	throttle
	server
	denied
	garbled
	slow
	cut
)

// Contract runs the shared retry and classification cases against a
// client: which failures are retried and counted (retries, throttles,
// timeouts, parse failures), exhaustion with its attempt count and class,
// requeue eligibility, failures that are not retried, and cancellation in
// a backoff.
func Contract(t *testing.T, w Wire, build func(Settings) Client, classify func(error) objhttp.Class) {
	for _, c := range []struct {
		name     string
		answers  []answer // per attempt; the last repeats
		attempts int      // MaxAttempts
		want     objhttp.Counts
		class    objhttp.Class // "" for success
		requeue  bool
	}{
		{"throttle, then a page", []answer{throttle, throttle, ok}, 9, objhttp.Counts{Attempts: 3, Retries: 2, Throttles: 2}, "", false},
		{"5xx, then a page", []answer{server, server, ok}, 9, objhttp.Counts{Attempts: 3, Retries: 2}, "", false},
		{"garbled body, then a page", []answer{garbled, ok}, 9, objhttp.Counts{Attempts: 2, Retries: 1, ParseFailures: 1}, "", false},
		{"timeout, then a page", []answer{slow, ok}, 9, objhttp.Counts{Attempts: 2, Retries: 1, Timeouts: 1}, "", false},
		{"cut connection, then a page", []answer{cut, ok}, 9, objhttp.Counts{Attempts: 2, Retries: 1}, "", false},
		{"5xx exhausted", []answer{server}, 3, objhttp.Counts{Attempts: 3, Retries: 2}, objhttp.ClassServer, true},
		// the exhausted call's last throttle or timeout is counted too
		{"throttle exhausted", []answer{throttle}, 2, objhttp.Counts{Attempts: 2, Retries: 1, Throttles: 2}, objhttp.ClassThrottle, true},
		{"timeout exhausted", []answer{slow}, 2, objhttp.Counts{Attempts: 2, Retries: 1, Timeouts: 2}, objhttp.ClassTimeout, true},
		{"cut exhausted", []answer{cut}, 2, objhttp.Counts{Attempts: 2, Retries: 1}, objhttp.ClassTransport, true},
		{"garbled exhausted", []answer{garbled}, 2, objhttp.Counts{Attempts: 2, Retries: 1, ParseFailures: 2}, objhttp.ClassParse, true},
		{"denied, not retried", []answer{denied}, 9, objhttp.Counts{Attempts: 1}, objhttp.ClassAccessDenied, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var n atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				i := int(n.Add(1)) - 1
				a := c.answers[min(i, len(c.answers)-1)]
				switch a {
				case ok:
					w.OK(rw)
				case throttle:
					w.Throttle(rw)
				case server:
					w.Server(rw)
				case denied:
					w.Denied(rw)
				case garbled:
					w.Garbled(rw)
				case slow:
					select {
					case <-time.After(5 * time.Second):
					case <-r.Context().Done():
					}
				case cut:
					conn, _, err := rw.(http.Hijacker).Hijack()
					if err == nil {
						conn.Close()
					}
				}
			}))
			defer srv.Close()
			cl := build(Settings{Endpoint: srv.URL, Timeout: 200 * time.Millisecond,
				Retry: objhttp.Retry{MaxAttempts: c.attempts, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}})
			err := cl.List(context.Background())
			got := cl.Counters()
			// one attempt at a time, each in flight for some time
			if want := min(c.want.Attempts, 1); got.InflightMax != want || (want > 0) != (got.InflightNs > 0) {
				t.Errorf("in flight: max %d, %d ns, want max %d", got.InflightMax, got.InflightNs, want)
			}
			got.InflightMax, got.InflightNs = 0, 0
			if got != c.want {
				t.Errorf("counters %+v, want %+v (err %v)", got, c.want, err)
			}
			if c.class == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var ex *objhttp.ExhaustedError
			if classify(err) != c.class || errors.As(err, &ex) != c.class.Transient() ||
				(ex != nil && ex.Attempts != c.attempts) || objhttp.Requeue(classify)(err) != c.requeue {
				t.Fatalf("err %v: class %q exhausted %v, want %q (requeue %v)", err, classify(err), ex != nil, c.class, c.requeue)
			}
		})
	}
	t.Run("limiter: throttles lower it, other failures do not", func(t *testing.T) {
		var n atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			switch n.Add(1) {
			case 1, 2:
				w.Throttle(rw)
			case 3:
				w.Server(rw)
			case 4:
				conn, _, err := rw.(http.Hijacker).Hijack()
				if err == nil {
					conn.Close()
				}
			default:
				w.OK(rw)
			}
		}))
		defer srv.Close()
		l := objhttp.NewLimiter(8)
		cl := build(Settings{Endpoint: srv.URL, Timeout: time.Second,
			Retry: objhttp.Retry{MaxAttempts: 9, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, Limiter: l}})
		if err := cl.List(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := cl.Counters()
		if l.Limit() != 3 || got.Retries != 3 || got.Limiter.Decreases != 2 || got.Limiter.Depth != 5 || got.Limiter.Increases != 0 {
			t.Fatalf("limit %d, counters %+v", l.Limit(), got)
		}
	})
	t.Run("canceled in a backoff", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) { w.Server(rw) }))
		defer srv.Close()
		cl := build(Settings{Endpoint: srv.URL, Timeout: time.Second, Retry: objhttp.Retry{MaxAttempts: 9, BaseBackoff: time.Hour, MaxBackoff: time.Hour}})
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := cl.List(ctx)
		if time.Since(start) > 2*time.Second || classify(err) != objhttp.ClassCanceled || objhttp.Requeue(classify)(err) {
			t.Fatalf("err %v class %q after %s", err, classify(err), time.Since(start))
		}
	})
}
