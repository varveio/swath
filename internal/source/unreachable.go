// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

// UnreachableGrace is how long a run whose clients have had no answer at
// all retries connections that fail deterministically (Unreachable)
// before it stops as unreachable: long enough for an emulator in a
// container that is still starting, short of the stall timeout a mistyped
// address would otherwise wait out. ArmUnreachable caps it at half an
// explicit --stall-timeout. A constant but for tests, which shorten it.
var UnreachableGrace = 30 * time.Second

// ErrUnreachable marks the failure that stops a run as unreachable
// (objhttp's fail-fast): nothing answered, and every connection failed
// deterministically for the grace. The command classifies it as stuck
// (exit 75), with cause CauseUnreachable.
var ErrUnreachable = errors.New("unreachable")

// CauseUnreachable is the outcome cause and failure class of
// ErrUnreachable.
const CauseUnreachable = "unreachable"

// Unreachable reports a connection that failed deterministically before
// any HTTP exchange: refused, the host not found, or no route to the
// network or host. Repeating it gives the same answer until someone starts
// the service or fixes the address. A timeout, a reset or a temporary DNS
// failure is not one; a refused certificate is already not retried
// (objhttp.Transport).
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

// The run's reachability, one state for the process (the command arms it
// for a run, as it sets the Watch): reachOff (not armed, or something
// answered: the hot path's one atomic load), reachArmed (nothing has
// answered or failed yet), reachExpired (the grace passed: every caller
// stops), or the Unix ns of the first deterministic connect failure.
// Contacted and ConnectFailed move it only by compare-and-swap, so once an
// answer is recorded no caller decides it expired.
var reach struct {
	state atomic.Int64
	first atomic.Int64      // the first failure's Unix ns, for the message
	grace atomic.Int64      // ns
	hook  func(state int64) // tests: between ConnectFailed's load and its swap
}

const (
	reachOff     int64 = 0
	reachArmed   int64 = -1
	reachExpired int64 = -2
)

// ArmUnreachable arms the run's fail-fast with UnreachableGrace, at most
// half of stall when it is positive (the stall watchdog's own warning
// point), so an explicitly smaller --stall-timeout is never exceeded.
// disarm turns it off at the run's end.
func ArmUnreachable(stall time.Duration) (grace time.Duration, disarm func()) {
	grace = UnreachableGrace
	if stall > 0 {
		grace = min(grace, stall/2)
	}
	reach.grace.Store(int64(grace))
	reach.first.Store(0)
	reach.state.Store(reachArmed)
	return grace, func() { reach.state.Store(reachOff) }
}

// Contacted records that an endpoint answered (any status: the response's
// headers arrived) or failed otherwise than deterministically: the
// fail-fast is off for the rest of the run, for every client.
func Contacted() {
	for {
		s := reach.state.Load()
		if s == reachOff || s == reachExpired {
			return
		}
		if reach.state.CompareAndSwap(s, reachOff) {
			return
		}
	}
}

// UnreachableOff reports the fail-fast off: not armed, or something in the
// run answered (one atomic load). An expired run is not off.
func UnreachableOff() bool { return reach.state.Load() == reachOff }

// UnreachableArmed reports whether nothing has answered in an armed run
// that has not expired, and its grace.
func UnreachableArmed() (time.Duration, bool) {
	s := reach.state.Load()
	return time.Duration(reach.grace.Load()), s != reachOff && s != reachExpired
}

// ConnectFailed records a deterministic connect failure (Unreachable) at
// now, and reports whether the run has expired: armed, nothing answered,
// and the first such failure a grace ago or more; since is the time from
// that first failure. Off, it costs one atomic load.
func ConnectFailed(now time.Time) (expired bool, since time.Duration) {
	ns := now.UnixNano()
	for {
		s := reach.state.Load()
		switch {
		case s == reachOff:
			return false, 0
		case s == reachExpired:
			return true, time.Duration(ns - reach.first.Load())
		case s == reachArmed:
			reach.first.CompareAndSwap(0, ns)
			reach.state.CompareAndSwap(reachArmed, reach.first.Load())
			continue
		}
		since = time.Duration(ns - s)
		if since < time.Duration(reach.grace.Load()) {
			return false, since
		}
		if h := reach.hook; h != nil {
			h(s)
		}
		if reach.state.CompareAndSwap(s, reachExpired) {
			return true, since
		}
	}
}
