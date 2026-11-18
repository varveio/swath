// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

// WalkRequest asks for every entry at or under a prefix.
type WalkRequest struct {
	// Prefix is the walk's scope: the keys that begin with it, its own
	// entry included when it exists. On an object store it is any key
	// prefix; on a local source it is empty, for the root, or a
	// directory's key.
	Prefix []byte
	// Workers is how many requests or directory reads run at once; zero
	// selects the backend's default. One selects the sequential mode: one
	// serial request chain, never split, the reference a parallel walk is
	// checked against. More workers spread a walk only when its scope is
	// larger than a page.
	Workers int
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
	// Unread are places the walk could not read and went on past, handed
	// on as the walk meets them; the walk keeps none of them.
	Unread []Unread
}

// WalkSummary is how a walk ended. Walk returns it with its error too: a
// walk that failed, or whose ctx ended, did not finish, and its Entries
// and Unread count what it handed to emit before it stopped. A walk
// finished when Evidence.Stop is StopCompleted; it read every entry under
// its prefix when it also handed on no Unread and Evidence.Unlocated is 0.
type WalkSummary struct {
	// Entries counts the entries handed to emit.
	Entries int64
	// Unread counts the Unread handed to emit.
	Unread int64
	// Evidence says where the entries came from and whether the walk
	// finished.
	Evidence Evidence
}
