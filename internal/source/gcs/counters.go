// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// The adapter's counters, after the client's.
const (
	cEndOmitted = int(objhttp.NumCounters) + iota
	cPoolSize
)

// clientCounters are the GCS client's counters (its Counts.Store fills
// them) and the adapter's, after the engine's.
var clientCounters = append(objhttp.CounterDefs("429 and rate-limit"),
	counters.Def{Key: "end_offset_omitted", Kind: counters.Count, Unit: "requests", Help: "bounded requests sent without endOffset (the bound would pass 1,024 bytes); the engine trims"},
	counters.Def{Key: "transport.pool_size", Kind: counters.Max, Unit: "transports", Help: "HTTP transports the built client spreads requests over, round-robin: one per 16 workers, 1 to 8 (one HTTP/2 connection each below the server's stream limit); 0 with a supplied Options.Client that does not report one"})
