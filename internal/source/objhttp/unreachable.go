// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// UnreachableGrace is how long a client retries an endpoint that has never
// answered while every attempt fails to connect deterministically
// (Unreachable): long enough for an emulator that is still starting, short
// of the stall timeout a mistyped address would otherwise wait out. A
// constant but for tests, which shorten it.
var UnreachableGrace = 15 * time.Second

// Unreachable reports a connection that failed deterministically before
// any HTTP exchange: refused, the host not found, or no route to the
// network or host. Repeating it gives the same answer until someone starts
// the service or fixes the address. A timeout, a reset or a temporary DNS
// failure is not one; a refused certificate is already not retried
// (Transport).
func Unreachable(err error) bool {
	if err == nil {
		return false
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && dns.IsNotFound {
		return true
	}
	for _, e := range unreachableErrnos {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// ClassUnreachable labels the fail-fast's failure: the endpoint never
// answered and every attempt failed to connect for UnreachableGrace.
const ClassUnreachable Class = "unreachable"

// UnreachableError is the failure that ends a call to an endpoint nothing
// has answered at (Counters.unreachable). Endpoint is the scheme and host
// only: a path or query may carry a credential.
type UnreachableError struct {
	Endpoint string
	For      time.Duration // since the first failed connect
	Err      error         // the last attempt's cause
}

func (e *UnreachableError) Error() string {
	return fmt.Sprintf("endpoint %s cannot be reached (%s): nothing answered there in %s; check the endpoint address, and that the service is running",
		e.Endpoint, innermost(e.Err), e.For.Round(100*time.Millisecond))
}
func (e *UnreachableError) Unwrap() error { return e.Err }

// innermost is err's innermost cause ("connection refused", "no such
// host"): the outer layers repeat the method and URL.
func innermost(err error) error {
	for u := errors.Unwrap(err); u != nil; u = errors.Unwrap(err) {
		err = u
	}
	return err
}

// unreachable applies the fail-fast to a failed attempt f (its request,
// t): while nothing has answered this client, an attempt that failed to
// connect deterministically (Unreachable) is retried as before for
// UnreachableGrace from the first one; past it, f becomes a fatal
// ClassUnreachable failure. Any other outcome (an answer of any status, a
// timeout, a reset) disarms it for the client's life: an endpoint that
// answered once and then refuses is an outage, retried until the run's
// stall timeout.
func (c *Counters) unreachable(f *Failure, t *target) *Failure {
	if c.reached.Load() {
		return f
	}
	if !Unreachable(f.Cause) {
		c.reached.Store(true)
		return f
	}
	c.connectFailures.Add(1)
	now := time.Now().UnixNano()
	c.unreachableSince.CompareAndSwap(0, now)
	since := time.Duration(now - c.unreachableSince.Load())
	if since < UnreachableGrace {
		return f
	}
	c.unreachableStops.Add(1)
	endpoint := "(unknown)"
	if u := t.url.Load(); u != nil {
		endpoint = u.Scheme + "://" + u.Host
	}
	return &Failure{Class: ClassUnreachable, Cause: &UnreachableError{Endpoint: endpoint, For: since, Err: f.Cause}}
}
