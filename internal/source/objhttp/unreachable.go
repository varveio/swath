// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"errors"
	"fmt"
	"time"

	"github.com/varveio/swath/internal/source"
)

// ClassUnreachable labels the fail-fast's failure (source.ErrUnreachable).
const ClassUnreachable Class = source.CauseUnreachable

// UnreachableError is the failure that stops a run nothing has answered
// in (Counters.unreachable): it is source.ErrUnreachable. Endpoint is the
// scheme and host only: a path or query may carry a credential.
type UnreachableError struct {
	Endpoint string
	For      time.Duration // since the run's first failed connect
	Err      error         // the last attempt's cause
}

func (e *UnreachableError) Error() string {
	return fmt.Sprintf("endpoint %s cannot be reached (%s): nothing has answered in %s; check the endpoint address, and that the service is running",
		e.Endpoint, innermost(e.Err), e.For.Round(100*time.Millisecond))
}
func (e *UnreachableError) Unwrap() error        { return e.Err }
func (e *UnreachableError) Is(target error) bool { return target == source.ErrUnreachable }

// innermost is err's innermost cause ("connection refused", "no such
// host"): the outer layers repeat the method and URL.
func innermost(err error) error {
	for u := errors.Unwrap(err); u != nil; u = errors.Unwrap(err) {
		err = u
	}
	return err
}

// unreachable applies the run's fail-fast (source.ConnectFailed) to a
// failed attempt f (its request, t): while the command has it armed and
// nothing in the run has answered, an attempt whose connection failed
// deterministically (source.Unreachable) is retried as before until the
// grace has passed since the run's first such failure; then f becomes a
// final ClassUnreachable failure. A credential failure never counts: its
// endpoint is not the one listed. Any answer, from any client, turns it
// off for the run (source.Contacted, from the Guard).
func (c *Counters) unreachable(f *Failure, t *target) *Failure {
	var ce *CredentialError
	if source.UnreachableOff() || !source.Unreachable(f.Cause) || errors.As(f.Cause, &ce) {
		return f
	}
	c.connectFailures.Add(1)
	expired, since := source.ConnectFailed(time.Now())
	if !expired {
		return f
	}
	c.unreachableStops.Add(1)
	endpoint := "(unknown)"
	if u := t.url.Load(); u != nil {
		endpoint = u.Scheme + "://" + u.Host
	}
	return &Failure{Class: ClassUnreachable, Cause: &UnreachableError{Endpoint: endpoint, For: since, Err: f.Cause}}
}
