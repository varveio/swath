// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "time"

// Budget bounds the work of one Walk or Summarize. A call that reaches a
// bound stops without an error: Evidence.Stop names the bound, and every
// extent a live call did not finish comes back as an Unread of class
// UnreadBudget. A negative bound is ErrInvalidRequest.
type Budget struct {
	// MaxEntries bounds the entries read. A walk counts the entries it
	// hands to emit, the prefix's own entry included, KindPrefix entries
	// and Unread excluded. Workers stop at the next batch boundary, so a
	// walk may deliver up to one batch per worker past the bound, and a
	// total it cuts short stays a lower bound. Zero sets no bound.
	MaxEntries int64
	// MaxDuration bounds the call's time, including the time emit takes.
	// Zero sets no bound. Unlike a deadline on ctx, which ends the call
	// with the context's error, reaching it returns what was read.
	MaxDuration time.Duration
	// MaxHeldBytes bounds the memory a live call holds for its own
	// checks, such as the names of each directory whose enumeration has
	// not ended. Zero selects the backend's default: the bound is never
	// lifted, and a directory too wide for it ends as an UnreadListing of
	// class UnreadBudget.
	MaxHeldBytes int64
}

// WalkRequest asks for every entry at or under a prefix.
type WalkRequest struct {
	// Prefix is the walk's scope: the keys that begin with it, its own
	// entry included when it exists. On a local source it is empty, for
	// the root, or a directory's key.
	Prefix []byte
	// MaxDepth bounds the levels below Prefix the walk reads: 1 reads
	// Prefix's own entry and its children. Zero reads every level. A walk
	// with a depth proves nothing below it. With a depth, an object store's
	// keys below it come back as KindPrefix entries.
	MaxDepth int
	// Workers is how many requests or directory reads run at once; zero
	// selects the backend's default. One selects the sequential mode: one
	// serial request chain, never split, the reference a parallel walk is
	// checked against. More workers spread a walk only when its scope is
	// larger than a page.
	Workers int
	// Budget bounds the walk.
	Budget Budget
}

// WalkBatch is one delivery of a walk to its emit callback. A backend
// calls emit from one goroutine, one batch at a time, in no order. A batch
// and every slice and value it reaches belong to the backend and are valid
// only until emit returns: copy what you keep. A slow emit blocks the
// walk's workers once the backend's queue is full; that is the walk's
// backpressure. An error from emit stops the walk: the workers are
// cancelled, the rest of the batch and anything queued are discarded, and
// Walk returns that error unchanged, never a short success.
type WalkBatch struct {
	// Entries are entries at or under the walk's prefix. Each is delivered
	// exactly once in the walk. A live source checks every entry's scope,
	// its key's lack of NUL, and an Object's or File's size before it
	// reaches emit.
	Entries []Entry
	// Unread are the extents the walk did not read that it met while
	// producing this batch.
	Unread []Unread
}

// WalkSummary is how a walk ended. A walk is complete when it returned no
// error, Evidence.Stop is StopCompleted, no batch held an Unread, and
// Evidence.Unlocated is 0: then every entry within its MaxDepth was read.
type WalkSummary struct {
	// Entries counts the entries handed to emit.
	Entries int64
	// Evidence says where the entries came from and how the walk ended.
	Evidence Evidence
}
