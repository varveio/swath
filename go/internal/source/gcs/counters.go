// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

const (
	cAttempts = iota
	cRetries
	cThrottles
	cTimeouts
	cParseFailures
	cEndOmitted
)

// clientCounters are the GCS client's counters, after the engine's.
var clientCounters = []counters.Def{
	{Key: "attempts", Kind: counters.Count, Unit: "requests", Help: "HTTP requests, retries included"},
	{Key: "retries", Kind: counters.Count, Unit: "requests", Help: "attempts repeated after a retryable failure"},
	{Key: "throttles", Kind: counters.Count, Unit: "requests", Help: "of the retries, 429 and rate-limit answers"},
	{Key: "timeouts", Kind: counters.Count, Unit: "requests", Help: "of the retries, attempts past -gcs-request-timeout"},
	{Key: "parse_failures", Kind: counters.Count, Unit: "responses", Help: "200 answers whose body did not parse (retried)"},
	{Key: "end_offset_omitted", Kind: counters.Count, Unit: "requests", Help: "bounded requests sent without endOffset (the bound would pass 1,024 bytes); the engine trims"},
}

// Counters is every counter a GCS listing reports.
var Counters = append(append([]counters.Def(nil), keyrange.Counters...), clientCounters...)
