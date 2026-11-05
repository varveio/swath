// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// The client's counters, indices of clientCounters (Provider.ID maps them).
const (
	cAttempts = iota
	cRetries
	cThrottles
	cTimeouts
	cParseFailures
)

// clientCounters follow the range engine's in the S3 tally.
var clientCounters = []counters.Def{
	{Key: "attempts", Kind: counters.Count, Unit: "requests", Help: "HTTP requests, retries included"},
	{Key: "retries", Kind: counters.Count, Unit: "requests", Help: "attempts repeated after a retryable failure"},
	{Key: "throttles", Kind: counters.Count, Unit: "requests", Help: "of the retries, 429/503/SlowDown answers"},
	{Key: "timeouts", Kind: counters.Count, Unit: "requests", Help: "of the retries, attempts past -request-timeout"},
	{Key: "parse_failures", Kind: counters.Count, Unit: "responses", Help: "200 answers whose body did not parse (retried)"},
}

// Counters is the S3 provider's tally, in report order: the range engine's
// counters, then the client's. The run report (source.stats.counters) and
// the table in docs/internals/counters.md are generated from it.
var Counters = append(append([]counters.Def(nil), keyrange.Counters...), clientCounters...)
