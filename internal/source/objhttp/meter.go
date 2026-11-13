// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import "github.com/varveio/swath/go/internal/source"

// Meter owns a built client's retry policy and request accounting. It must
// not be copied after use.
type Meter struct {
	Retry Retry
	Tally Counters
}

// NewMeter applies retry defaults and returns a fresh request account.
func NewMeter(r Retry) *Meter {
	r.Defaults()
	return &Meter{Retry: r}
}

// Answered is the number of requests the service answered usably.
func (m *Meter) Answered() int64 { return m.Tally.Answered() }

// Live is the request account with the limiter's current limit.
func (m *Meter) Live() source.Requests {
	l := m.Tally.Live()
	l.Limit = int64(m.Retry.Limiter.Limit())
	return l
}

// Counters returns the request counts, including limiter and rate activity.
func (m *Meter) Counters() Counts {
	n := m.Tally.Load()
	n.Limiter = m.Retry.Limiter.Counts()
	n.Rate = m.Retry.Rate.Counts()
	return n
}
