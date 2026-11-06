// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// cEndOmitted is the adapter's counter, after the client's.
const cEndOmitted = int(objhttp.NumCounters)

// clientCounters are the GCS client's counters (its Counts.Store fills
// them) and the adapter's, after the engine's.
var clientCounters = append(objhttp.CounterDefs("429 and rate-limit", "-gcs-request-timeout"),
	counters.Def{Key: "end_offset_omitted", Kind: counters.Count, Unit: "requests", Help: "bounded requests sent without endOffset (the bound would pass 1,024 bytes); the engine trims"})

// Counters is every counter a GCS listing reports.
var Counters = append(append([]counters.Def(nil), keyrange.Counters...), clientCounters...)
