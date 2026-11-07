// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package faulttest is a fault-injecting HTTP proxy for tests and
// benchmarks of the object-store clients: it sits in front of a listing
// server (swath-replay, a fake bucket) and answers a share of requests with
// a throttle (503 SlowDown for S3, 429 rateLimitExceeded for GCS), delays
// them, or resets their connection, in time windows, deterministically from
// a seed: request n of a run gets the same faults for the same plan. A rate
// cap throttles the requests beyond a given rate, the shape of a real
// per-prefix request limit. It records a timeline of what reached it, for
// checking how a client's concurrency reacts. Never linked into the swath
// binary.
package faulttest

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Fault is what the proxy does to a chosen request.
type Fault int

const (
	Throttle Fault = iota // the dialect's throttle answer, never forwarded
	Latency               // forwarded after Delay
	Reset                 // the connection is reset before any answer
)

func (f Fault) String() string { return [...]string{"throttle", "latency", "reset"}[f] }

// Window applies a fault to a share of the requests that arrive in it.
type Window struct {
	From, To time.Duration // since the run's first request; To 0 = until the end
	// N0 and N1 bound the window by request number instead, [N0, N1) in
	// arrival order from 0, N1 0 = no bound; with From and To, both
	// apply. A test that must see its faults whatever the host's load
	// uses them: how many requests arrive in a time span is up to the
	// scheduler.
	N0, N1   uint64
	Fraction float64 // share of the window's requests that get the fault (1 = all)
	Fault    Fault
	Delay    time.Duration // Latency: added before the request is forwarded
}

func (w Window) holds(n uint64, t time.Duration) bool {
	return t >= w.From && (w.To == 0 || t < w.To) && n >= w.N0 && (w.N1 == 0 || n < w.N1)
}

// Plan is a run's faults.
type Plan struct {
	Seed    uint64
	Dialect string // "s3" (default) or "gcs": the throttle answer's shape
	Windows []Window
	// RateCap, if positive, throttles every request beyond RateCap in the
	// trailing second (served requests only count).
	RateCap float64
}

// Bucket is one Step of the timeline.
type Bucket struct {
	Arrived   int64 // requests that arrived
	Throttled int64 // of them, answered with the throttle
	Reset     int64 // of them, reset
	Delayed   int64 // of them, delayed
	MaxActive int64 // most requests in the proxy at once (all kinds), from arrival until the answer is released
}

// Stats are a run's totals.
type Stats struct {
	Requests, Throttled, Reset, Delayed, Forwarded int64
	MaxActive                                      int64
}

// Step is the timeline's resolution.
const Step = 100 * time.Millisecond

// Proxy is the fault-injecting proxy, an http.Handler. It forwards each
// answer whole, buffered, never streamed (Bucket.MaxActive).
type Proxy struct {
	plan  Plan
	next  *httputil.ReverseProxy
	seq   atomic.Uint64
	start atomic.Int64 // UnixNano of the first request
	now   func() time.Time
	// answered, if set, runs after a request's answer is released, in
	// the handler: tests stall it to stand for a descheduled handler.
	answered func()

	mu       sync.Mutex
	active   int64
	timeline []Bucket
	served   []time.Time // the rate cap's trailing second
	stats    Stats
}

// New builds a proxy that forwards to target (http://host:port, or an
// https:// provider endpoint, which is sent its own host name as Host:
// a virtual-hosted service routes and signs by it).
func New(target string, plan Plan) (*Proxy, error) {
	u, err := url.Parse(target)
	if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("faulttest: target %q: want http://host:port or https://host", target)
	}
	if plan.Dialect == "" {
		plan.Dialect = "s3"
	}
	if plan.Dialect != "s3" && plan.Dialect != "gcs" {
		return nil, fmt.Errorf("faulttest: dialect %q: want s3 or gcs", plan.Dialect)
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns, t.MaxIdleConnsPerHost = 0, 1024
	rp := httputil.NewSingleHostReverseProxy(u)
	rp.Transport = t
	if u.Scheme == "https" {
		direct := rp.Director
		rp.Director = func(r *http.Request) {
			direct(r)
			r.Host = u.Host
		}
	}
	return &Proxy{plan: plan, next: rp, now: time.Now}, nil
}

// Draw is the deterministic uniform [0, 1) value of request n for window i.
func Draw(seed, n uint64, i int) float64 {
	h := fnv.New64a()
	var b [24]byte
	binary.LittleEndian.PutUint64(b[0:], seed)
	binary.LittleEndian.PutUint64(b[8:], n)
	binary.LittleEndian.PutUint64(b[16:], uint64(i))
	h.Write(b[:])
	return float64(h.Sum64()>>11) / (1 << 53)
}

// Decide returns the faults of request n arriving at t since the run's
// first request (the windows' faults, independent per window; a throttle
// or reset ends the request, so the first of those wins).
func (p *Plan) Decide(n uint64, t time.Duration) (throttle, reset bool, delay time.Duration) {
	for i, w := range p.Windows {
		if !w.holds(n, t) || Draw(p.Seed, n, i) >= w.Fraction {
			continue
		}
		switch w.Fault {
		case Throttle:
			if !reset {
				throttle = true
			}
		case Reset:
			if !throttle {
				reset = true
			}
		case Latency:
			delay += w.Delay
		}
	}
	return throttle, reset, delay
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := p.now()
	p.start.CompareAndSwap(0, now.UnixNano())
	t := now.Sub(time.Unix(0, p.start.Load()))
	n := p.seq.Add(1) - 1
	throttle, reset, delay := p.plan.Decide(n, t)

	p.mu.Lock()
	if !throttle && !reset && p.plan.RateCap > 0 {
		cut := 0
		for cut < len(p.served) && now.Sub(p.served[cut]) >= time.Second {
			cut++
		}
		p.served = p.served[cut:]
		if float64(len(p.served)) >= p.plan.RateCap {
			throttle = true
		} else {
			p.served = append(p.served, now)
		}
	}
	p.active++
	b := p.bucket(t)
	b.Arrived++
	b.MaxActive = max(b.MaxActive, p.active)
	p.stats.Requests++
	p.stats.MaxActive = max(p.stats.MaxActive, p.active)
	switch {
	case throttle:
		b.Throttled++
		p.stats.Throttled++
	case reset:
		b.Reset++
		p.stats.Reset++
	default:
		if delay > 0 {
			b.Delayed++
			p.stats.Delayed++
		}
		p.stats.Forwarded++
	}
	p.mu.Unlock()
	// The request leaves the count before its answer can reach the client:
	// a client that sends its next request as soon as it has an answer (a
	// one-permit limiter) must never find the previous one still counted.
	// Decrementing when the handler returns instead raced the client under
	// load: a reset is visible the moment the connection closes, and the
	// retry could arrive before the handler's deferred decrement ran.
	leave := sync.OnceFunc(func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	})
	defer leave() // a panic or an abandoned request included

	switch {
	case throttle:
		leave()
		p.throttle(w)
	case reset:
		leave()
		resetConn(w)
	default:
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		// Buffered, so the whole answer is held until the request has
		// left the count (a listing page is small).
		rec := httptest.NewRecorder()
		p.next.ServeHTTP(rec, r)
		leave()
		h := w.Header()
		for k, v := range rec.Header() {
			h[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}
	if p.answered != nil {
		p.answered()
	}
}

// bucket is the timeline bucket of t (p.mu held).
func (p *Proxy) bucket(t time.Duration) *Bucket {
	i := int(t / Step)
	for len(p.timeline) <= i {
		p.timeline = append(p.timeline, Bucket{})
	}
	return &p.timeline[i]
}

func (p *Proxy) throttle(w http.ResponseWriter) {
	if p.plan.Dialect == "gcs" {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"code":429,"message":"The rate of requests is too high.",`+
			`"errors":[{"message":"The rate of requests is too high.","domain":"usageLimits","reason":"rateLimitExceeded"}]}}`)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+
		`<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message><RequestId>faulttest</RequestId></Error>`)
}

// resetConn resets the request's connection (TCP RST where it can).
func resetConn(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	c, _, err := hj.Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = c.Close()
}

// Stats returns the totals so far.
func (p *Proxy) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// Timeline returns a copy of the timeline so far, one Bucket per Step
// since the first request.
func (p *Proxy) Timeline() []Bucket {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Bucket(nil), p.timeline...)
}

// TSV renders a timeline: t_ms, arrived, throttled, reset, delayed, max_active.
func TSV(tl []Bucket) string {
	var b strings.Builder
	b.WriteString("t_ms\tarrived\tthrottled\treset\tdelayed\tmax_active\n")
	for i, x := range tl {
		fmt.Fprintf(&b, "%d\t%d\t%d\t%d\t%d\t%d\n", i*int(Step/time.Millisecond), x.Arrived, x.Throttled, x.Reset, x.Delayed, x.MaxActive)
	}
	return b.String()
}

// ParseWindow parses "FAULT:FROM-TO:FRACTION[:DELAY]" (durations in Go
// syntax, TO empty = until the end), for the bench command's flags:
// throttle:10s-25s:0.5, latency:0s-:1:200ms, reset:5s-6s:0.3.
func ParseWindow(s string) (Window, error) {
	f := strings.Split(s, ":")
	if len(f) < 3 || len(f) > 4 {
		return Window{}, fmt.Errorf("window %q: want FAULT:FROM-TO:FRACTION[:DELAY]", s)
	}
	var w Window
	switch f[0] {
	case "throttle":
		w.Fault = Throttle
	case "latency":
		w.Fault = Latency
	case "reset":
		w.Fault = Reset
	default:
		return Window{}, fmt.Errorf("window %q: fault %q: want throttle, latency or reset", s, f[0])
	}
	from, to, ok := strings.Cut(f[1], "-")
	if !ok {
		return Window{}, fmt.Errorf("window %q: want FROM-TO", s)
	}
	var err error
	if w.From, err = time.ParseDuration(from); err != nil {
		return Window{}, fmt.Errorf("window %q: %w", s, err)
	}
	if to != "" {
		if w.To, err = time.ParseDuration(to); err != nil {
			return Window{}, fmt.Errorf("window %q: %w", s, err)
		}
	}
	if w.Fraction, err = strconv.ParseFloat(f[2], 64); err != nil || w.Fraction < 0 || w.Fraction > 1 {
		return Window{}, fmt.Errorf("window %q: fraction %q: want 0..1", s, f[2])
	}
	if len(f) == 4 {
		if w.Delay, err = time.ParseDuration(f[3]); err != nil {
			return Window{}, fmt.Errorf("window %q: %w", s, err)
		}
	}
	if w.Fault == Latency && w.Delay <= 0 {
		return Window{}, fmt.Errorf("window %q: a latency window needs a DELAY", s)
	}
	return w, nil
}
