// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "time"

// Target is one thing to summarize: the entry at exactly Key, or, with
// Subtree, every entry whose key begins with Key, Key's own included. An
// object at exactly "a/" and everything under "a/" are different targets.
// On an object store a subtree's Key is any key prefix; on a local source
// it is empty, for the root, or a directory's key.
type Target struct {
	// Key is the target's key.
	Key []byte
	// Subtree selects every entry at or under Key instead of exactly Key.
	Subtree bool
}

// SummaryOptions say what to count.
type SummaryOptions struct {
	// Kinds are the kinds counted. Empty counts files and objects
	// (KindFile and KindObject); name the kinds to count others, such as
	// KindDir for directories' own sizes. KindPrefix is never counted.
	Kinds []Kind
}

// Totals measure the counted entries of a set. Counts are one per
// directory entry, with no hard-link dedupe. A total that a counted entry
// cannot supply is nil, never a partial sum, and so is a sum that would
// overflow.
type Totals struct {
	// Entries is the number of counted entries.
	Entries int64
	// Bytes is the sum of their Size; nil when one has no Size.
	Bytes *int64
	// AllocatedBytes is the sum of their AllocatedBytes; nil when one has
	// none.
	AllocatedBytes *int64
	// Newest is the latest of their LastModified; nil when one has none,
	// or when none is counted.
	Newest *time.Time
}

// TargetSummary is one target's answer. It says what the target's key
// itself is, so that zero counted entries tells an empty directory from a
// symbolic link, an absent key and an unread one, and whether its totals
// cover everything at or under it.
type TargetSummary struct {
	// State is what is at exactly the target's Key, as Lookup answers it.
	State LookupState
	// Self is the entry at exactly Key when State is LookupPresent.
	Self *Entry
	// Reason says why State is LookupUnknown, as LookupResult.Reason does.
	Reason error
	// Totals count the target's entries of SummaryOptions.Kinds. An entry
	// counts toward every target that holds it, so overlapping targets are
	// each counted in full. Where the target is not Complete, the totals
	// cover only what was read.
	Totals
	// Complete says every entry at or under the target was read: its own
	// entry's state is known (the root's, which is no entry, excepted), no
	// place under it went unread, and the source has no unlocated gap.
	Complete bool
	// Unread counts the places under the target that the call could not
	// read and went on past, as a Walk of it hands them on. Walk names
	// them.
	Unread int64
}
