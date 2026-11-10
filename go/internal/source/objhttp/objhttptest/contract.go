// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package objhttptest is the retry and classification contract every
// object-store client on objhttp passes: each client's tests run Contract
// with its own wire answers.
package objhttptest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
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
	Endpoint   string // the test server's base URL
	Retry      objhttp.Retry
	Timeout    time.Duration // per attempt
	HTTPClient *http.Client  // supplied transport and redirect policy
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
	cutBody
	bodyDeadline
	badGzip
	redirect307
	redirect308
)

// Contract runs the shared retry and classification cases against a
// client: which failures are retried and counted (retries, throttles,
// timeouts, parse failures), exhaustion with its attempt count and stored signals,
// requeue eligibility, failures that are not retried, and cancellation in
// a backoff.
func Contract(t *testing.T, w Wire, build func(Settings) Client) {
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
		{"5xx exhausted", []answer{server}, 3, objhttp.Counts{Attempts: 3, Retries: 2}, objhttp.ClassOther, true},
		// the exhausted call's last throttle or timeout is counted too
		{"throttle exhausted", []answer{throttle}, 2, objhttp.Counts{Attempts: 2, Retries: 1, Throttles: 2}, objhttp.ClassThrottle, true},
		{"timeout exhausted", []answer{slow}, 2, objhttp.Counts{Attempts: 2, Retries: 1, Timeouts: 2}, objhttp.ClassTimeout, true},
		{"cut exhausted", []answer{cut}, 2, objhttp.Counts{Attempts: 2, Retries: 1}, objhttp.ClassOther, true},
		{"garbled exhausted", []answer{garbled}, 2, objhttp.Counts{Attempts: 2, Retries: 1, ParseFailures: 2}, objhttp.ClassParse, true},
		{"cut success body, then a page", []answer{cutBody, ok}, 9, objhttp.Counts{Attempts: 2, Retries: 1}, "", false},
		{"cut success body exhausted", []answer{cutBody}, 2, objhttp.Counts{Attempts: 2, Retries: 1}, objhttp.ClassOther, true},
		{"body deadline, then a page", []answer{bodyDeadline, ok}, 9, objhttp.Counts{Attempts: 2, Retries: 1, Timeouts: 1}, "", false},
		{"body deadline exhausted", []answer{bodyDeadline}, 2, objhttp.Counts{Attempts: 2, Retries: 1, Timeouts: 2}, objhttp.ClassTimeout, true},
		{"malformed gzip", []answer{badGzip}, 9, objhttp.Counts{Attempts: 1}, objhttp.ClassOther, false},
		{"307 redirect refused", []answer{redirect307}, 9, objhttp.Counts{Attempts: 1}, objhttp.ClassOther, false},
		{"308 redirect refused", []answer{redirect308}, 9, objhttp.Counts{Attempts: 1}, objhttp.ClassOther, false},
		{"denied, not retried", []answer{denied}, 9, objhttp.Counts{Attempts: 1}, objhttp.ClassAccessDenied, false},
	} {
		modes := []bool{true}
		if c.answers[0] == redirect307 || c.answers[0] == redirect308 {
			modes = append(modes, false)
		}
		for _, supplied := range modes {
			t.Run(fmt.Sprintf("%s/supplied=%v", c.name, supplied), func(t *testing.T) {
				var n, followed, answered atomic.Int64
				target := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) { followed.Add(1); w.OK(rw) }))
				defer target.Close()
				srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
					i := int(n.Add(1)) - 1
					a := c.answers[min(i, len(c.answers)-1)]
					if a != cut && a != slow {
						answered.Add(1)
					}
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
					case cutBody:
						rw.Header().Set("Content-Length", "100000")
						w.Garbled(rw)
					case bodyDeadline:
						w.Garbled(rw)
						rw.(http.Flusher).Flush()
						<-r.Context().Done()
					case badGzip:
						rw.Header().Set("Content-Encoding", "gzip")
						fmt.Fprint(rw, "not gzip at all")
					case redirect307, redirect308:
						status := 307
						if a == redirect308 {
							status = 308
						}
						http.Redirect(rw, r, target.URL, status)
					case cut:
						conn, _, err := rw.(http.Hijacker).Hijack()
						if err == nil {
							conn.Close()
						}
					}
				}))
				defer srv.Close()
				bodies := &bodyCloses{next: http.DefaultTransport}
				var hc *http.Client
				if supplied {
					hc = &http.Client{Transport: bodies}
				}
				cl := build(Settings{Endpoint: srv.URL, Timeout: 200 * time.Millisecond, HTTPClient: hc,
					Retry: objhttp.Retry{MaxAttempts: c.attempts, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}})
				err := cl.List(context.Background())
				if supplied {
					bodies.check(t, int(answered.Load()))
				}
				if followed.Load() != 0 || n.Load() != c.want.Attempts {
					t.Fatalf("%d listing requests, %d redirects followed", n.Load(), followed.Load())
				}
				got := cl.Counters()
				// one attempt at a time, each in flight for some time
				if want := min(c.want.Attempts, 1); got.InflightMax != want || mistimed(want > 0, got.InflightNs) {
					t.Errorf("in flight: max %d, %d ns, want max %d", got.InflightMax, got.InflightNs, want)
				}
				// every attempt is timed: ordered percentiles, a timed-out one at the top
				if mistimed(c.want.Attempts > 0, got.P50Ns) || got.P50Ns > got.P90Ns || got.P90Ns > got.P99Ns ||
					(c.want.Timeouts > 0 && got.P99Ns < int64(200*time.Millisecond)) {
					t.Errorf("attempt latency p50 %d, p90 %d, p99 %d ns", got.P50Ns, got.P90Ns, got.P99Ns)
				}
				got.InflightMax, got.InflightNs, got.P50Ns, got.P90Ns, got.P99Ns = 0, 0, 0, 0, 0
				if got != c.want {
					t.Errorf("counters %+v, want %+v (err %v)", got, c.want, err)
				}
				if c.class == "" {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				var f *objhttp.Failure
				var ex *objhttp.ExhaustedError
				if !errors.As(err, &f) {
					t.Fatalf("error has no normalized failure: %v", err)
				}
				if f.Class != c.class || f.Transient != c.requeue ||
					f.Throttle != (c.want.Throttles > 0) || f.Timeout != (c.want.Timeouts > 0) ||
					errors.As(err, &ex) != c.requeue || (ex != nil && (ex.Attempts != c.attempts || ex.Err != f)) ||
					objhttp.Requeue(err) != c.requeue || objhttp.Normalize(err) != f {
					t.Fatalf("err %v: failure %+v exhausted %v, want %q (requeue %v)", err, f, ex != nil, c.class, c.requeue)
				}
				last := c.answers[len(c.answers)-1]
				if (last == cutBody || last == bodyDeadline || last == badGzip) && (f.Status != 0 || f.Code != "") {
					t.Fatalf("wire failure became a service answer: %+v", f)
				}
				if (last == redirect307 && f.Status != 307) || (last == redirect308 && f.Status != 308) {
					t.Fatalf("redirect lost status: %+v", f)
				}
				if (last == throttle || last == server || last == denied) && (f.Status == 0 || f.Code == "" || (last == denied && f.Status != 403)) {
					t.Fatalf("service answer lost its HTTP status: %+v", f)
				}
			})
		}
	}
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel after headers/deadline=%v", deadline), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				w.Garbled(rw)
				rw.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer srv.Close()
			ctx, cancel := context.WithCancel(t.Context())
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			bodies := &bodyCloses{next: http.DefaultTransport}
			if !deadline {
				bodies.afterHeaders = cancel
			}
			cl := build(Settings{Endpoint: srv.URL, Timeout: 5 * time.Second, HTTPClient: &http.Client{Transport: bodies}, Retry: objhttp.Retry{MaxAttempts: 3}})
			err := cl.List(ctx)
			got := cl.Counters()
			if !errors.Is(err, want) || objhttp.Classify(err) != objhttp.ClassOther || objhttp.Requeue(err) || got.Attempts != 1 || got.Retries != 0 || got.Timeouts != 0 || got.ParseFailures != 0 {
				t.Fatalf("error %v, counters %+v", err, got)
			}
			bodies.check(t, 1)
		})
	}
	// A limiter of 8 has a 256-answer window (the floor): the 39th throttle
	// reaches 15 % of it and cuts (8 -> 5, the window emptied); the 40th is
	// tolerated.
	t.Run("limiter: throttles lower it, other failures do not", func(t *testing.T) {
		var n atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			switch k := n.Add(1); {
			case k <= 40:
				w.Throttle(rw)
			case k == 41:
				w.Server(rw)
			case k == 42:
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
			Retry: objhttp.Retry{MaxAttempts: 45, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, Limiter: l}})
		if err := cl.List(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := cl.Counters()
		if l.Limit() != 5 || got.Retries != 41 || got.Limiter.Decreases != 1 || got.Limiter.Tolerated != 39 || got.Limiter.Depth != 3 || got.Limiter.Increases != 0 {
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
		if time.Since(start) > 2*time.Second || !errors.Is(err, context.DeadlineExceeded) || objhttp.Requeue(err) {
			t.Fatalf("err %v class %q after %s", err, objhttp.Classify(err), time.Since(start))
		}
	})
}

// coarseClock: the monotonic clock advances in ticks of about half a
// millisecond (Windows), so a request to a local server can take 0 ns.
var coarseClock = runtime.GOOS == "windows"

// mistimed reports a mismatch between whether something ran (ran) and its
// measured duration ns: nothing ran and time was measured, or it ran and
// none was, where the clock can see it.
func mistimed(ran bool, ns int64) bool {
	return (!ran && ns != 0) || (ran && ns == 0 && !coarseClock)
}

// bodyCloses observes the original bodies beneath each client's Guard.
type bodyCloses struct {
	next         http.RoundTripper
	afterHeaders func()
	mu           sync.Mutex
	bodies       []*closedBody
}

func (c *bodyCloses) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	b := &closedBody{ReadCloser: resp.Body}
	resp.Body = b
	c.mu.Lock()
	c.bodies = append(c.bodies, b)
	c.mu.Unlock()
	if c.afterHeaders != nil {
		c.afterHeaders()
	}
	return resp, nil
}
func (c *bodyCloses) check(t *testing.T, want int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.bodies) != want {
		t.Errorf("%d original bodies, want %d", len(c.bodies), want)
	}
	for i, b := range c.bodies {
		if got := b.closes.Load(); got != 1 {
			t.Errorf("body %d closed %d times", i, got)
		}
	}
}

type closedBody struct {
	io.ReadCloser
	closes atomic.Int64
}

func (b *closedBody) Close() error { b.closes.Add(1); return b.ReadCloser.Close() }
