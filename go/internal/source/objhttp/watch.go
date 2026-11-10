// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"net/url"
	"sync/atomic"
	"time"
)

// Exchange is one HTTP round trip as a Guard saw it, its body read
// included: the command's request log and its persistent-failure warning
// read it.
type Exchange struct {
	Method string
	// URL is the request's. Its query may carry a credential (an Azure
	// SAS): a reader shows it redacted.
	URL     *url.URL
	Status  int   // 0 without a response
	Err     error // the transport's error (no response), nil with one
	Latency time.Duration
}

// Failed is one failed attempt of Do: retried after Wait, or the last
// one (Retry false: not worth repeating, or MaxAttempts reached).
type Failed struct {
	Attempt int // 1 for the first
	Failure *Failure
	Retry   bool
	Wait    time.Duration
}

// Watch receives every client's exchanges and Do's failed attempts,
// process-wide; either may be nil. Both are called on the request path:
// they must return promptly.
type Watch struct {
	Exchange func(Exchange)
	Failed   func(Failed)
}

var watching atomic.Pointer[Watch]

// SetWatch installs w for every client built or running (nil: none).
func SetWatch(w *Watch) { watching.Store(w) }

// failed tells the watch, if any, of a failed attempt.
func failed(n int, f *Failure, retry bool, wait time.Duration) {
	if w := watching.Load(); w != nil && w.Failed != nil {
		w.Failed(Failed{Attempt: n, Failure: f, Retry: retry, Wait: wait})
	}
}
