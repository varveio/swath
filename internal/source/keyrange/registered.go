// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import "github.com/varveio/swath/internal/source"

// Registered is what a provider on the range engine gives the pipeline
// beyond registry.Bound, the one list of the optional interfaces of package
// source that the pipeline probes for on a registered source, by type
// assertion. A provider's opened source embeds it, set to the provider, so
// these methods are promoted and nothing else of the provider is (not its
// Root, Expand, Replay, Stats, Options, ID or counter Set); a provider
// missing one fails to compile at that assignment, where an absent method
// would otherwise silently turn its capability off.
//
// The provider satisfies it, not its Engine: the request activity
// (Answered, Requests, Faults) is supplied by the provider's Activity;
// Restart is promoted from its Engine.
type Registered interface {
	Listed() int64 // registry.Source
	source.Live
	source.Advancing
	source.Requested
	source.SeedTimed
	source.Faulted
	source.Contended
	source.Resumable
	source.Seeded
	source.Reusing
	source.Fencing
}
