// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

// Admission is how a provider's own HTTP client admits its attempts, the
// same for every provider on objhttp: the adaptive limiter (with its slow
// start) and the --request-rate cap. It is transport configuration, apart
// from the range engine's policy; the engine only reports its features
// (Off).
type Admission struct {
	// Limiter: attempts wait for a permit of an adaptive limiter of at most
	// the worker count (Limiter).
	Limiter bool
	// SlowStart: with Limiter, the limit starts at 4 and doubles per second
	// until the worker count or the first throttle or timeout
	// (NewSlowStartLimiter).
	SlowStart bool
	// RequestRate caps attempts per second (Rate); 0: no cap.
	RequestRate float64
}

// New builds the client's limiter (nil: none) and rate cap (nil: none)
// for workers permits: the one construction every provider uses.
func (a Admission) New(workers int) (*Limiter, *Rate) {
	var lim *Limiter
	switch {
	case a.Limiter && a.SlowStart:
		lim = NewSlowStartLimiter(workers)
	case a.Limiter:
		lim = NewLimiter(workers)
	}
	return lim, NewRate(a.RequestRate)
}

// Off names the admission features a run has off, for the report's
// feature states: all of them for a client the provider did not build
// (own false: a supplied client has no admission of ours).
func (a Admission) Off(own bool) []string {
	var off []string
	if !own || !a.Limiter {
		off = append(off, FeatureLimiter)
	}
	if !own || !a.Limiter || !a.SlowStart {
		off = append(off, FeatureSlowStart)
	}
	if !own || a.RequestRate <= 0 {
		off = append(off, FeatureRequestRate)
	}
	return off
}

// The admission features, as the report's features name them.
const (
	FeatureLimiter     = "limiter"
	FeatureSlowStart   = "slow_start"
	FeatureRequestRate = "request_rate"
)
