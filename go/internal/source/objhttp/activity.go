// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import "github.com/varveio/swath/go/internal/source"

// Activity is a listing client's request activity as the pipeline reads
// it live (source.Faulted, source.Requested, source.Live). A provider
// embeds it, made by Observe once its client is chosen: the built client
// counts everything; a supplied client may count any part of it or none,
// and each part it does not count reads zero.
type Activity struct {
	counted  interface{ Counters() Counts }
	live     interface{ Live() Live }
	answered interface{ Answered() int64 }
}

// Observe is client's activity, through whichever of Counters, Live and
// Answered it implements.
func Observe(client any) Activity {
	var a Activity
	a.counted, _ = client.(interface{ Counters() Counts })
	a.live, _ = client.(interface{ Live() Live })
	a.answered, _ = client.(interface{ Answered() int64 })
	return a
}

// Faults are the throttle answers and attempt timeouts so far
// (source.Faulted).
func (a Activity) Faults() (throttles, timeouts int64) {
	if a.counted == nil {
		return 0, 0
	}
	n := a.counted.Counters()
	return n.Throttles, n.Timeouts
}

// Requests is the client's live request account (source.Requested).
func (a Activity) Requests() source.Requests {
	if a.live == nil {
		return source.Requests{}
	}
	l := a.live.Live()
	return source.Requests{Attempts: l.Attempts, Retries: l.Retries, Throttles: l.Throttles, Timeouts: l.Timeouts, Inflight: l.Inflight,
		InflightMax: l.InflightMax, InflightNs: l.InflightNs, Limit: int64(l.Limit)}
}

// Answered is the requests the service answered usably (source.Live).
func (a Activity) Answered() int64 {
	if a.answered == nil {
		return 0
	}
	return a.answered.Answered()
}
