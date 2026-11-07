// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package objhttp is the HTTP support the object-store listing clients
// share: the retry loop (attempts, full-jitter backoff, exhaustion), the
// failure classes and transport classification, the client counters and
// route canonicalization. What a service's answers mean (status codes,
// error codes and reasons), its payload parsing and its authentication stay
// in each client.
package objhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"math/rand/v2"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/counters"
)

// Class is what kind of failure an error is, for the caller's decision:
// retry the request later or stop the run. It is the run outcome's cause.
type Class string

const (
	ClassAuth         Class = "auth"           // credentials missing, unusable, refused or not issued
	ClassAccessDenied Class = "access_denied"  // the principal may not list this bucket
	ClassNoSuchBucket Class = "no_such_bucket" // the bucket does not exist (or not at this endpoint)
	ClassRedirect     Class = "redirect"       // the bucket is in another region or endpoint
	ClassThrottle     Class = "throttle"       // the service asked the client to slow down
	ClassTimeout      Class = "timeout"        // an attempt exceeded the request timeout
	ClassServer       Class = "server"         // another 5xx
	ClassTransport    Class = "transport"      // the connection failed or was cut
	ClassParse        Class = "parse"          // a 200 whose body did not parse
	ClassInvalidPage  Class = "invalid_page"   // a page that breaks the listing contract (provider-detected)
	ClassCanceled     Class = "canceled"       // the caller's context ended
	ClassOther        Class = "other"
)

// Transient reports whether a request that failed this way may succeed
// later: the caller retries it after a backoff and never fails the run on
// it (liveness is the command's watchdog's decision).
func (c Class) Transient() bool {
	switch c {
	case ClassThrottle, ClassTimeout, ClassServer, ClassTransport, ClassParse:
		return true
	}
	return false
}

// Answer is a service's error answer, as a client parsed it.
type Answer interface {
	error
	Throttle() bool  // the service asked the client to slow down
	Retryable() bool // the request may succeed when repeated
}

// CredentialError is a credential failure: credentials that do not load,
// or a token or key the provider would not issue. Transient ones (a token
// endpoint's 5xx, a cut connection, an attempt past the request timeout)
// are retried; the others stop the request.
type CredentialError struct {
	Source    string // where the credentials came from (never the secret)
	Transient bool
	Timeout   bool // the credential did not arrive within the request timeout
	Err       error
}

func (e *CredentialError) Error() string {
	return "credentials (" + e.Source + "): " + e.Err.Error()
}
func (e *CredentialError) Unwrap() error { return e.Err }

// ExhaustedError is a transient failure that outlasted the client's own
// retries. The caller decides what happens next.
type ExhaustedError struct {
	Attempts int
	Err      error
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("after %d attempts: %v", e.Attempts, e.Err)
}
func (e *ExhaustedError) Unwrap() error { return e.Err }

var (
	// ErrSyntax marks a 200 whose body did not parse (a cut or garbled
	// body): retried, never skipped.
	ErrSyntax = errors.New("malformed listing response")
	// ErrInvalidPage marks a page that parsed but breaks the listing
	// contract (out of order, truncated without a token); the range engine
	// wraps it.
	ErrInvalidPage = errors.New("invalid page")
	// ErrTimeout marks an attempt that ran out of its own time.
	ErrTimeout = errors.New("request timed out")
)

// Timeout marks err as an attempt timeout when the attempt's context
// (actx) ran out of its own time and the caller's (ctx) did not.
func Timeout(actx, ctx context.Context, limit time.Duration, err error) error {
	if ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w after %s: %v", ErrTimeout, limit, err)
	}
	return err
}

// Transport reports a connection failure worth repeating (not a
// certificate refusal).
func Transport(err error) bool {
	var cert *tls.CertificateVerificationError
	if errors.As(err, &cert) {
		return false
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset")
}

// Failure is a failed attempt normalized once, for every decision taken on
// it: whether the client repeats it, its cause (the outcome's class), and
// the admission signal the limiter takes from it.
type Failure struct {
	Class    Class
	Retry    bool // the client repeats the attempt (after a backoff)
	Throttle bool // the service asked the client to slow down
	Timeout  bool // the attempt, or its credential, ran out of time
}

// Normalize is err's Failure, with answer the client's mapping of its own
// service answers (nil: a generic one from the Answer interface). The
// admission signal follows the class, so an answer a client maps to a
// timeout (a GCS 408, an S3 RequestTimeout) slows the limiter's growth as
// an attempt past the request timeout does.
func Normalize(err error, answer func(error) (Class, bool)) Failure {
	if answer == nil {
		answer = genericAnswer
	}
	c := Classify(err, answer)
	return Failure{Class: c, Retry: retryable(err), Throttle: c == ClassThrottle, Timeout: c == ClassTimeout}
}

// genericAnswer maps an Answer by its own predicates.
func genericAnswer(err error) (Class, bool) {
	var a Answer
	switch {
	case !errors.As(err, &a):
		return "", false
	case a.Throttle():
		return ClassThrottle, true
	case a.Retryable():
		return ClassServer, true
	}
	return ClassOther, true
}

// retryable reports whether the client repeats a failed attempt.
func retryable(err error) bool {
	var ce *CredentialError
	var a Answer
	switch {
	case errors.As(err, &ce):
		return ce.Transient
	case errors.As(err, &a):
		return a.Retryable()
	case errors.Is(err, ErrTimeout), errors.Is(err, ErrSyntax):
		return true
	}
	return Transport(err)
}

// Classify returns err's class: a credential failure (auth; one that may
// pass, such as a token endpoint's outage, is timeout or transport, so that
// its exhaustion is requeued like any other and never fails a run), the
// caller's cancellation, a contract break, then the service's answer (answer, the
// client's mapping of its own Answer type; ok false when err holds none),
// a parse failure, an attempt timeout, a transport failure, other.
func Classify(err error, answer func(error) (Class, bool)) Class {
	var ce *CredentialError
	switch {
	case err == nil:
		return ""
	case errors.As(err, &ce):
		// before the context check: a credential failure may wrap a
		// deadline (a token exchange's own timeout) without the caller's
		// context having ended (Do returns the caller's cancellation bare)
		switch {
		case !ce.Transient:
			return ClassAuth
		case ce.Timeout:
			return ClassTimeout
		}
		return ClassTransport
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ClassCanceled
	case errors.Is(err, ErrInvalidPage):
		return ClassInvalidPage
	}
	if c, ok := answer(err); ok {
		return c
	}
	switch {
	case errors.Is(err, ErrSyntax):
		return ClassParse
	case errors.Is(err, ErrTimeout):
		return ClassTimeout
	case Transport(err):
		return ClassTransport
	}
	return ClassOther
}

// Requeue reports a transient failure whose client retries were exhausted:
// the range engine asks the page again (keyrange.Description.Requeue).
func Requeue(class func(error) Class) func(error) bool {
	return func(err error) bool {
		var ex *ExhaustedError
		return errors.As(err, &ex) && class(err).Transient()
	}
}

// Retry is a client's retry policy and admission. Zero values take the
// defaults.
type Retry struct {
	MaxAttempts int           // default 9
	BaseBackoff time.Duration // default 100 ms
	MaxBackoff  time.Duration // default 5 s
	// Limiter, if set, admits every attempt (Limiter); nil admits all.
	Limiter *Limiter
	// Rate, if set, caps attempts per second (Rate): each takes a token
	// after its permit.
	Rate *Rate
	// Answer is the client's mapping of its service answers to classes
	// (Normalize; nil: the generic one).
	Answer func(error) (Class, bool)
}

// Defaults fills the zero values.
func (r *Retry) Defaults() {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = 9
	}
	if r.BaseBackoff <= 0 {
		r.BaseBackoff = 100 * time.Millisecond
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = 5 * time.Second
	}
}

// Do runs attempt until it succeeds, fails in a way not worth repeating,
// or has run MaxAttempts times (then an *ExhaustedError), waiting a
// full-jitter exponential backoff between attempts and counting retries,
// throttles and timeouts (every throttled or timed-out attempt, the last
// one of an exhausted call included). With r.Limiter, each attempt waits for a permit
// first and returns it with its outcome and latency; no permit is held
// during a backoff. The caller's cancellation ends it at once.
func Do[T any](ctx context.Context, r Retry, c *Counters, attempt func() (T, error)) (T, error) {
	var zero T
	for n := 1; ; n++ {
		v, admitted, err := admit(ctx, r, attempt)
		if err == nil {
			c.answered.Add(1)
			return v, nil
		}
		if ctx.Err() != nil || !admitted {
			return zero, ctx.Err()
		}
		f := Normalize(err, r.Answer)
		if f.Throttle {
			c.throttles.Add(1)
		}
		if f.Timeout {
			c.timeouts.Add(1)
		}
		if !f.Retry || n >= r.MaxAttempts {
			if f.Retry {
				err = &ExhaustedError{Attempts: n, Err: err}
			}
			return zero, err
		}
		c.retries.Add(1)
		d := min(r.MaxBackoff, r.BaseBackoff<<min(n-1, 30))
		t := time.NewTimer(rand.N(d) + 1)
		select {
		case <-ctx.Done():
			t.Stop()
			return zero, ctx.Err()
		case <-t.C:
		}
	}
}

// admit runs one attempt under l's admission (none when l is nil), then
// rt's cap (none when nil); admitted is false when the caller's context
// ended while it waited.
func admit[T any](ctx context.Context, r Retry, attempt func() (T, error)) (v T, admitted bool, err error) {
	l, rt := r.Limiter, r.Rate
	if l == nil {
		if err := rt.Wait(ctx); err != nil {
			return v, false, err
		}
		v, err = attempt()
		return v, true, err
	}
	if err := l.Acquire(ctx); err != nil {
		return v, false, err
	}
	if err := rt.Wait(ctx); err != nil {
		l.Release(Ignored, 0)
		return v, false, err
	}
	t0 := time.Now() // after the waits: admission is not latency
	// The permit goes back however the attempt ends, a panic included
	// (as Ignored: no signal); a recovered caller keeps a working limiter.
	o := Ignored
	defer func() { l.Release(o, time.Since(t0)) }()
	v, err = attempt()
	if err == nil {
		o = Success
		return v, true, nil
	}
	switch f := Normalize(err, r.Answer); {
	case f.Throttle:
		o = Throttled
	case f.Timeout:
		o = TimedOut
	}
	return v, true, err
}

// Counts are what a client did, for the run summary.
type Counts struct {
	Attempts  int64 `json:"attempts"`  // HTTP requests sent
	Retries   int64 `json:"retries"`   // attempts repeated after a retryable failure
	Throttles int64 `json:"throttles"` // attempts answered with a throttle, retried or not
	Timeouts  int64 `json:"timeouts"`  // attempts past the per-request timeout, retried or not
	// ParseFailures counts 200 responses whose body did not parse (a cut or
	// garbled body); they are retried like transport faults, never dropped.
	ParseFailures int64 `json:"parse_failures"`
	// InflightMax is the most attempts in flight at once (admitted, from
	// send to body read); InflightNs sums their time in flight, so
	// InflightNs over the listing's wall time is the mean in flight.
	InflightMax int64 `json:"inflight_max"`
	InflightNs  int64 `json:"inflight_ns"`
	// P50Ns, P90Ns and P99Ns are attempt latency percentiles (send to body
	// read, every attempt), from a log-spaced histogram (latencyHist): the
	// upper edge of the percentile's bucket, at most 25% above it, or
	// LatencyOpen when the percentile falls among the attempts at or past
	// the histogram's range (2^40 ns), which LatencyOverflow counts.
	P50Ns           int64 `json:"p50_ns"`
	P90Ns           int64 `json:"p90_ns"`
	P99Ns           int64 `json:"p99_ns"`
	LatencyOverflow int64 `json:"latency_overflow"`
	// Limiter is the admission's engagements (zero without a limiter).
	Limiter LimiterCounts `json:"limiter"`
	// Rate is the --request-rate cap's (zero without one).
	Rate RateCounts `json:"rate"`
}

// Counters accumulate Counts; safe for concurrent use.
type Counters struct {
	attempts, retries, throttles, timeouts, parseFailures atomic.Int64
	answered                                              atomic.Int64
	inflight, inflightMax, inflightNs                     atomic.Int64
	latency                                               latencyHist
}

// latencyHist is a fixed log-spaced histogram of durations in ns: four
// buckets per power of two (each at most a quarter of its lower edge
// wide), from 1 ns to 2^40 ns (about 18 minutes 20 s: past any page a
// healthy route serves, and an attempt that long is reported, not
// resolved), then one open-ended overflow bucket for longer attempts. One
// atomic per bucket: recording takes no lock.
type latencyHist struct{ b [latencyBuckets]atomic.Int64 }

const (
	latencyMaxBits  = 40
	latencyFinite   = (latencyMaxBits-2)*4 + 4 // buckets with a finite upper edge, the last one's 2^40-1 ns
	latencyOverflow = latencyFinite            // the open bucket: 2^40 ns and longer
	latencyBuckets  = latencyFinite + 1
)

// LatencyOpen is the percentile reported when it falls in the overflow
// bucket: its upper edge is unbounded, so the gauge carries the largest
// value it can rather than a finite edge that would read as a bound.
const LatencyOpen = math.MaxInt64

// latencyBucket is d's bucket: d itself below 4, else 4·(octave-2) plus
// the two bits after the leading one; latencyOverflow at 2^40 ns and up.
func latencyBucket(d int64) int {
	if d < 4 {
		return int(max(d, 0))
	}
	n := bits.Len64(uint64(d))
	if n > latencyMaxBits {
		return latencyOverflow
	}
	return (n-2)*4 + int((d>>(n-3))&3)
}

// latencyUpper is bucket i's upper edge (inclusive), LatencyOpen for the
// overflow bucket.
func latencyUpper(i int) int64 {
	if i == latencyOverflow {
		return LatencyOpen
	}
	if i < 4 {
		return int64(i)
	}
	n, sub := i/4+2, int64(i%4)
	return (5+sub)<<(n-3) - 1
}

func (h *latencyHist) record(d time.Duration) { h.b[latencyBucket(int64(d))].Add(1) }

// quantiles are the upper edges of the buckets holding the qs quantiles
// (0 without samples).
func (h *latencyHist) quantiles(qs ...float64) []int64 {
	var counts [latencyBuckets]int64
	var total int64
	for i := range counts {
		counts[i] = h.b[i].Load()
		total += counts[i]
	}
	out := make([]int64, len(qs))
	if total == 0 {
		return out
	}
	for j, q := range qs {
		rank := int64(math.Ceil(q * float64(total)))
		var seen int64
		for i, c := range counts {
			if seen += c; seen >= rank {
				out[j] = latencyUpper(i)
				break
			}
		}
	}
	return out
}

// Attempt counts an HTTP request sent, and in flight until the returned
// func is called (after its body is read): call it just before the send.
func (c *Counters) Attempt() (done func()) {
	c.attempts.Add(1)
	t0 := time.Now()
	n := c.inflight.Add(1)
	for m := c.inflightMax.Load(); n > m && !c.inflightMax.CompareAndSwap(m, n); m = c.inflightMax.Load() {
	}
	return func() {
		d := time.Since(t0)
		c.inflight.Add(-1)
		c.inflightNs.Add(int64(d))
		c.latency.record(d)
	}
}

// Answered is the number of requests the service answered usably (an
// attempt that returned no error): the run's liveness between rows, not
// part of Counts.
func (c *Counters) Answered() int64 { return c.answered.Load() }

// Live is a client's request account now, for the progress sampler and
// the end-of-run summary: atomics only, nothing on the request path.
type Live struct {
	Attempts, Retries, Throttles int64
	Inflight                     int64 // attempts in flight now
	InflightMax, InflightNs      int64 // as Counts
	Limit                        int   // the limiter's current limit (0 without one)
}

// Live reads the counters (Limit is the client's to fill).
func (c *Counters) Live() Live {
	return Live{Attempts: c.attempts.Load(), Retries: c.retries.Load(), Throttles: c.throttles.Load(), Inflight: c.inflight.Load(),
		InflightMax: c.inflightMax.Load(), InflightNs: c.inflightNs.Load()}
}

// ParseFailure counts a 200 whose body did not parse.
func (c *Counters) ParseFailure() { c.parseFailures.Add(1) }

// Load returns the counts so far.
func (c *Counters) Load() Counts {
	q := c.latency.quantiles(0.50, 0.90, 0.99)
	return Counts{Attempts: c.attempts.Load(), Retries: c.retries.Load(), Throttles: c.throttles.Load(),
		Timeouts: c.timeouts.Load(), ParseFailures: c.parseFailures.Load(), InflightMax: c.inflightMax.Load(),
		InflightNs: c.inflightNs.Load(), P50Ns: q[0], P90Ns: q[1], P99Ns: q[2], LatencyOverflow: c.latency.b[latencyOverflow].Load()}
}

// ClientCounter names a client counter: its offset in CounterDefs and in
// a provider's tally after the engine's (Store). NumCounters, the end
// sentinel, is the count: a new counter is one constant before it, one
// definition and one Store line, and TestCounterTable refuses a gap.
type ClientCounter int

const (
	CAttempts ClientCounter = iota
	CRetries
	CThrottles
	CTimeouts
	CParseFailures
	CInflightMax
	CInflightNs
	CInflightP50
	CInflightP90
	CInflightP99
	CLatencyOverflow
	CLimiterDecreases
	CLimiterFloorThrottles
	CLimiterIncreases
	CLimiterCooldown
	CLimiterLatencyFrozen
	CLimiterTimeoutFrozen
	CLimiterValve
	CLimiterWaits
	CLimiterWaitNs
	CLimiterDepth
	CSlowStartDoubles
	CSlowStartNs
	CSlowStartEnd
	CRateWaits
	CRateWaitNs
	NumCounters // the end sentinel: the number of client counters
)

// CounterDefs are the client counters' definitions, by ClientCounter, after
// the engine's in a provider's tally (Store fills them); throttles and
// timeouts name the provider's own answers and flag. The limiter's carry
// the feature "limiter", its slow start's "slow_start" (on only with both),
// the rate cap's "request_rate". Names follow the engine's: a dotted group,
// a snake_case leaf, high-water marks prefixed max_, durations suffixed _ns.
func CounterDefs(throttles, timeoutFlag string) []counters.Def {
	const f = FeatureLimiter
	d := make([]counters.Def, NumCounters)
	d[CAttempts] = counters.Def{Key: "attempts", Kind: counters.Count, Unit: "requests", Help: "HTTP requests sent: every attempt, client retries and engine requeues included (the request economics)"}
	d[CRetries] = counters.Def{Key: "retries", Kind: counters.Count, Unit: "requests", Help: "attempts repeated after a retryable failure"}
	d[CThrottles] = counters.Def{Key: "throttles", Kind: counters.Count, Unit: "requests", Help: "attempts answered " + throttles + ", retried or not (the last of an exhausted call included)"}
	d[CTimeouts] = counters.Def{Key: "timeouts", Kind: counters.Count, Unit: "requests", Help: "attempts past " + timeoutFlag + ", retried or not"}
	d[CParseFailures] = counters.Def{Key: "parse_failures", Kind: counters.Count, Unit: "responses", Help: "200 answers whose body did not parse (retried)"}
	d[CInflightMax] = counters.Def{Key: "inflight.max_active", Kind: counters.Max, Unit: "requests", Help: "most attempts in flight at once (admitted, send to body read)"}
	d[CInflightNs] = counters.Def{Key: "inflight.busy_ns", Kind: counters.Count, Unit: "ns", Help: "attempts' time in flight, summed: over the listing's wall time, the mean in flight"}
	for id, q := range map[ClientCounter]string{CInflightP50: "50", CInflightP90: "90", CInflightP99: "99"} {
		d[id] = counters.Def{Key: "inflight.p" + q + "_ns", Kind: counters.Gauge, Unit: "ns", Help: "attempt latency (send to body read), " + q + "th percentile: the upper edge of its bucket in a log-spaced histogram, at most 25% above; 9223372036854775807 (open) when it falls past 2^40 ns"}
	}
	d[CLatencyOverflow] = counters.Def{Key: "inflight.latency_overflow", Kind: counters.Count, Unit: "attempts", Help: "attempts at or past 2^40 ns (about 18 min 20 s), the latency histogram's open last bucket"}
	d[CLimiterDecreases] = counters.Def{Key: "limiter.decreases", Kind: counters.Count, Unit: "steps", Feature: f, Help: "limit multiplied by 0.7 on a " + throttles + " answer"}
	d[CLimiterFloorThrottles] = counters.Def{Key: "limiter.floor_throttles", Kind: counters.Count, Unit: "answers", Feature: f, Help: throttles + " answers at a limit of 1: no decrease and no fresh 10 s cool-down"}
	d[CLimiterIncreases] = counters.Def{Key: "limiter.increases", Kind: counters.Count, Unit: "steps", Feature: f, Help: "limit raised by one (at most one per second, after 10 s without a throttle)"}
	d[CLimiterCooldown] = counters.Def{Key: "limiter.growth_cooldown", Kind: counters.Count, Unit: "steps", Feature: f, Help: "due growth steps held back: a throttle answer in the last 10 s"}
	d[CLimiterLatencyFrozen] = counters.Def{Key: "limiter.growth_latency_frozen", Kind: counters.Count, Unit: "steps", Feature: f, Help: "due growth steps held back: successful attempts' latency EWMA above twice the baseline"}
	d[CLimiterTimeoutFrozen] = counters.Def{Key: "limiter.growth_timeout_frozen", Kind: counters.Count, Unit: "steps", Feature: f, Help: "due growth steps held back: 3 or more attempt timeouts in the last 10 s"}
	d[CLimiterValve] = counters.Def{Key: "limiter.valve", Kind: counters.Count, Unit: "steps", Feature: f, Help: "limit raised by one through the latency freeze (at most one per 30 s, with progress)"}
	d[CLimiterWaits] = counters.Def{Key: "limiter.waits", Kind: counters.Count, Unit: "attempts", Feature: f, Help: "attempts that waited for a permit"}
	d[CLimiterWaitNs] = counters.Def{Key: "limiter.wait_ns", Kind: counters.Count, Unit: "ns", Feature: f, Help: "time those attempts waited, summed"}
	d[CLimiterDepth] = counters.Def{Key: "limiter.max_depth", Kind: counters.Max, Unit: "permits", Feature: f, Help: "most permits the limit stood below the worker count"}
	d[CSlowStartDoubles] = counters.Def{Key: "slow_start.doubles", Kind: counters.Count, Unit: "steps", Feature: FeatureSlowStart, Help: "--limiter-slow-start: limit doubled (from 4, at most once per second, before the first throttle or timeout)"}
	d[CSlowStartNs] = counters.Def{Key: "slow_start.ramp_ns", Kind: counters.Max, Unit: "ns", Feature: FeatureSlowStart, Help: "--limiter-slow-start: time from the start to the ramp's end (the worker count reached, or the first throttle or timeout)"}
	d[CSlowStartEnd] = counters.Def{Key: "slow_start.end_limit", Kind: counters.Max, Unit: "permits", Feature: FeatureSlowStart, Help: "--limiter-slow-start: the limit the ramp ended at (0: still ramping)"}
	d[CRateWaits] = counters.Def{Key: "rate.waits", Kind: counters.Count, Unit: "attempts", Feature: FeatureRequestRate, Help: "attempts that waited for a --request-rate token"}
	d[CRateWaitNs] = counters.Def{Key: "rate.wait_ns", Kind: counters.Count, Unit: "ns", Feature: FeatureRequestRate, Help: "time those attempts waited, summed"}
	return d
}

// Store writes n into set at the IDs of CounterDefs, offset by first.
func (n Counts) Store(set *counters.Set, first counters.ID) {
	put := func(id ClientCounter, v int64) { set.Set(first+counters.ID(id), v) }
	l := n.Limiter
	put(CAttempts, n.Attempts)
	put(CRetries, n.Retries)
	put(CThrottles, n.Throttles)
	put(CTimeouts, n.Timeouts)
	put(CParseFailures, n.ParseFailures)
	put(CInflightMax, n.InflightMax)
	put(CInflightNs, n.InflightNs)
	put(CInflightP50, n.P50Ns)
	put(CInflightP90, n.P90Ns)
	put(CInflightP99, n.P99Ns)
	put(CLatencyOverflow, n.LatencyOverflow)
	put(CLimiterDecreases, l.Decreases)
	put(CLimiterFloorThrottles, l.FloorThrottles)
	put(CLimiterIncreases, l.Increases)
	put(CLimiterCooldown, l.Cooldown)
	put(CLimiterLatencyFrozen, l.LatencyFrozen)
	put(CLimiterTimeoutFrozen, l.TimeoutFrozen)
	put(CLimiterValve, l.Valve)
	put(CLimiterWaits, l.Waits)
	put(CLimiterWaitNs, l.WaitNs)
	put(CLimiterDepth, l.Depth)
	put(CSlowStartDoubles, l.SlowStartDoubles)
	put(CSlowStartNs, l.SlowStartNs)
	put(CSlowStartEnd, l.SlowStartEnd)
	put(CRateWaits, n.Rate.Waits)
	put(CRateWaitNs, n.Rate.WaitNs)
}

// CanonicalRoute is the allowlist of equivalent route spellings for
// resume identity: the scheme and host are case-insensitive; the path is
// kept byte for byte (a client folds its endpoint's trailing slash once,
// before this).
func CanonicalRoute(r string) string {
	i := strings.Index(r, "://")
	if i < 0 {
		return r
	}
	host, path, cut := strings.Cut(r[i+3:], "/")
	r = strings.ToLower(r[:i]) + "://" + strings.ToLower(host)
	if cut {
		r += "/" + path
	}
	return r
}
