// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"net/url"
	"sync/atomic"
	"time"
)

// Exchange is one HTTP round trip as a provider's request guard saw it,
// its body read included: the command's request log and its
// persistent-failure warning read it.
type Exchange struct {
	Method string
	// URL is the request's. Its query may carry a credential (an Azure
	// SAS): a reader shows it redacted.
	URL     *url.URL
	Status  int   // 0 without a response
	Err     error // the transport's error (no response), nil with one
	Latency time.Duration
}

// Failed is one failed attempt of a provider's retry loop: retried after
// Wait, or the last one (Retry false: not worth repeating, or the
// attempts exhausted). Class, Status, Code and Cause are the normalized
// failure's.
type Failed struct {
	Attempt int // 1 for the first
	Class   string
	Status  int
	Code    string // the provider's code or reason
	Cause   error
	Retry   bool
	Wait    time.Duration
	URL     *url.URL // the attempt's request (nil: none was sent)
}

// Watch receives every client's exchanges and failed attempts,
// process-wide; either may be nil. Both are called on the request path:
// they must return promptly.
type Watch struct {
	Exchange func(Exchange)
	Failed   func(Failed)
}

var watching atomic.Pointer[Watch]

// SetWatch installs w for every client built or running (nil: none).
func SetWatch(w *Watch) { watching.Store(w) }

// Watching is the watch installed (nil: none).
func Watching() *Watch { return watching.Load() }
