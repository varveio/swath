// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import "github.com/varveio/swath/go/internal/source/objhttp"

// clientCounters follow the range engine's in the S3 tally (the client's
// Counts.Store fills them).
var clientCounters = objhttp.CounterDefs("429/503/SlowDown")
