// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// clientCounters follow the range engine's in the S3 tally (the client's
// Counts.Store fills them).
var clientCounters = objhttp.CounterDefs("429/503/SlowDown", "-request-timeout")

// Counters is the S3 provider's tally, in report order: the range engine's
// counters, then the client's. The run report (source.stats.counters) and
// the table in docs/internals/counters.md are generated from it.
var Counters = append(append([]counters.Def(nil), keyrange.Counters...), clientCounters...)
