// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source"
)

// Report is an engine provider's run report (source.Reporter): its stats
// (which hold c), the brief line from the engine's counters, and coverage
// of target, complete unless -max-keys stopped the listing.
func Report(name, target, prefix string, stats any, c counters.Snapshot) source.Report {
	v := c.Value
	truncated := v("truncated_at_max_keys") != 0
	r := source.Report{Name: name, Stats: stats,
		Brief: fmt.Sprintf("objects=%d bytes=%d pages=%d requests=%d retries=%d splits=%d child_empty=%d max_active=%d truncated=%v",
			v("objects"), v("bytes"), v("pages"), v("requests"), v("retries"), v("split.accepted_bounded")+v("split.accepted_frontier"),
			v("child_empty.lower")+v("child_empty.upper"), v("max_active"), truncated),
		Coverage: source.Coverage{Target: target, Prefix: prefix, Complete: !truncated, StopReason: "completed"},
	}
	if truncated {
		r.Coverage.StopReason = "max_keys"
	}
	return r
}

// Capabilities are an engine provider's (source.Capable): no rank oracle,
// a fabricable seek over byte-ordered keys, so the range engine applies;
// a sequential run lists one token chain. resumable is the provider's
// decision (a capped run, or a supplied client that cannot name what
// identity freezes, cannot resume).
func Capabilities(sequential, resumable bool) source.Capabilities {
	c := source.Capabilities{Resume: resumable, Seek: source.SeekFabricable, Order: source.OrderBytes}
	if sequential {
		c.Seek = source.SeekTokenOnly
	}
	return c
}
