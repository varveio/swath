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

// retryable classifies a failed attempt for the loop.
func retryable(err error) (retry, throttle, timeout bool) {
	var ce *CredentialError
	var a Answer
	switch {
	case errors.As(err, &ce):
		return ce.Transient, false, ce.Timeout
	case errors.As(err, &a):
		return a.Retryable(), a.Throttle(), false
	case errors.Is(err, ErrTimeout):
		return true, false, true
	case errors.Is(err, ErrSyntax):
		return true, false, false
	}
	return Transport(err), false, false
}

// Classify returns err's class: the caller's cancellation, a contract
// break, a credential failure (auth; one that may pass, such as a token
// endpoint's outage, is timeout or transport, so that its exhaustion is
// requeued like any other and never fails a run), then the service's answer (answer, the
// client's mapping of its own Answer type; ok false when err holds none),
// a parse failure, an attempt timeout, a transport failure, other.
func Classify(err error, answer func(error) (Class, bool)) Class {
	var ce *CredentialError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ClassCanceled
	case errors.Is(err, ErrInvalidPage):
		return ClassInvalidPage
	case errors.As(err, &ce):
		switch {
		case !ce.Transient:
			return ClassAuth
		case ce.Timeout:
			return ClassTimeout
		}
		return ClassTransport
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

// Retry is a client's retry policy. Zero values take the defaults.
type Retry struct {
	MaxAttempts int           // default 9
	BaseBackoff time.Duration // default 100 ms
	MaxBackoff  time.Duration // default 5 s
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
// throttles and timeouts. The caller's cancellation ends it at once.
func Do[T any](ctx context.Context, r Retry, c *Counters, attempt func() (T, error)) (T, error) {
	var zero T
	for n := 1; ; n++ {
		v, err := attempt()
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		retry, throttle, timeout := retryable(err)
		if !retry || n >= r.MaxAttempts {
			if retry {
				err = &ExhaustedError{Attempts: n, Err: err}
			}
			return zero, err
		}
		c.retries.Add(1)
		if throttle {
			c.throttles.Add(1)
		}
		if timeout {
			c.timeouts.Add(1)
		}
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

// Counts are what a client did, for the run summary.
type Counts struct {
	Attempts  int64 `json:"attempts"`  // HTTP requests sent
	Retries   int64 `json:"retries"`   // attempts repeated after a retryable failure
	Throttles int64 `json:"throttles"` // of those, throttling answers
	Timeouts  int64 `json:"timeouts"`  // of those, attempts past the per-request timeout
	// ParseFailures counts 200 responses whose body did not parse (a cut or
	// garbled body); they are retried like transport faults, never dropped.
	ParseFailures int64 `json:"parse_failures"`
}

// Counters accumulate Counts; safe for concurrent use.
type Counters struct {
	attempts, retries, throttles, timeouts, parseFailures atomic.Int64
}

// Attempt counts an HTTP request sent.
func (c *Counters) Attempt() { c.attempts.Add(1) }

// ParseFailure counts a 200 whose body did not parse.
func (c *Counters) ParseFailure() { c.parseFailures.Add(1) }

// Load returns the counts so far.
func (c *Counters) Load() Counts {
	return Counts{c.attempts.Load(), c.retries.Load(), c.throttles.Load(), c.timeouts.Load(), c.parseFailures.Load()}
}

// CounterDefs are the client counters' definitions, after the engine's in a
// provider's tally (Store fills them in this order); throttles and
// timeouts name the provider's own answers and flag.
func CounterDefs(throttles, timeoutFlag string) []counters.Def {
	return []counters.Def{
		{Key: "attempts", Kind: counters.Count, Unit: "requests", Help: "HTTP requests, retries included"},
		{Key: "retries", Kind: counters.Count, Unit: "requests", Help: "attempts repeated after a retryable failure"},
		{Key: "throttles", Kind: counters.Count, Unit: "requests", Help: "of the retries, " + throttles + " answers"},
		{Key: "timeouts", Kind: counters.Count, Unit: "requests", Help: "of the retries, attempts past " + timeoutFlag},
		{Key: "parse_failures", Kind: counters.Count, Unit: "responses", Help: "200 answers whose body did not parse (retried)"},
	}
}

// NumCounters is len(CounterDefs(...)).
const NumCounters = 5

// Store writes n into set at the IDs of CounterDefs, the first at first.
func (n Counts) Store(set *counters.Set, first counters.ID) {
	for i, v := range [NumCounters]int64{n.Attempts, n.Retries, n.Throttles, n.Timeouts, n.ParseFailures} {
		set.Set(first+counters.ID(i), v)
	}
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
