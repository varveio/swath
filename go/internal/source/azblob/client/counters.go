// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// The client's own counters' offsets in CounterDefs, after objhttp's:
// transport.pool_size, retry_after_capped and body_cap_shrinks.
const (
	CPoolSize = int(objhttp.NumCounters) + iota
	CRetryAfterCapped
	CBodyCapShrinks
)

// CounterDefs are the client's counters, as GCS declares its own: objhttp's
// (attempts, retries, throttles, timeouts, parse_failures and the
// admission's; Counters().Store fills them), then transport.pool_size
// (Transports), retry_after_capped (RetryAfterCapped) and
// body_cap_shrinks (BodyCapShrinks).
var CounterDefs = append(objhttp.CounterDefs("503 ServerBusy and 429", "-azblob-request-timeout"),
	counters.Def{Key: "transport.pool_size", Kind: counters.Max, Unit: "transports", Help: "HTTP transports the built client spreads requests over, round-robin: one per 16 workers, 1 to 8 (one HTTP/2 connection each below the server's stream limit); 0 with a supplied client that does not report one"},
	counters.Def{Key: "retry_after_capped", Kind: counters.Count, Unit: "answers", Help: "answers whose Retry-After exceeded the request timeout: the wait before the next attempt was cut to it"},
	counters.Def{Key: "body_cap_shrinks", Kind: counters.Count, Unit: "requests", Help: "responses past the 64 MiB body cap: the page was asked again with maxresults halved"})
