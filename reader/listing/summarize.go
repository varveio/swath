// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "time"

// Target is one thing an inventory's Summarize counts: the entry at
// exactly Key, or, with Subtree, every entry whose key begins with Key,
// Key's own included. An object at exactly "a/" and everything under "a/"
// are different targets. In an object store's inventory a subtree's Key is
// any key prefix; in a local one it is empty, for the root, or a
// directory's key.
type Target struct {
	// Key is the target's key.
	Key []byte
	// Subtree selects every entry at or under Key instead of exactly Key.
	Subtree bool
}

// SummaryOptions say what to count, and how to split it.
type SummaryOptions struct {
	// Kinds are the kinds counted. Empty counts files and objects
	// (KindFile and KindObject); name the kinds to count others, such as
	// KindDir for directories' own sizes. KindPrefix is never counted.
	Kinds []Kind
	// Split also divides each target's totals into groups.
	Split Split
}

// Split names the properties that divide a target's counted entries into
// groups. The zero Split asks for none.
type Split struct {
	// StorageClass groups by Entry.StorageClass. A local inventory has
	// none, so all its entries are in groups with no class.
	StorageClass bool
	// SizeBounds groups by size band, to tell how many entries and bytes
	// lie in each; nil or empty asks for no bands. The bounds are
	// positive and strictly ascending; others are ErrInvalidRequest.
	// Band 0 holds the sizes below SizeBounds[0], band i the sizes below
	// SizeBounds[i] and not below SizeBounds[i-1], and band
	// len(SizeBounds) the rest. A size equal to a bound is in the band
	// above it.
	SizeBounds []int64
}

// Group is the totals of one group of a target's counted entries.
type Group struct {
	// StorageClass is the group's class when the split names it; nil for
	// entries with none, and when it does not.
	StorageClass *string
	// SizeBand is the group's band when the split has bounds, -1 for
	// entries with no Size; zero when it has none.
	SizeBand int
	// Totals are the group's, by the rules of a target's.
	Totals
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
// symbolic link, an absent key and an unknown one, and whether its totals
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
	// Groups are Totals divided as SummaryOptions.Split asks. Every
	// counted entry is in exactly one group, and a group with no entry is
	// left out. They are ordered by StorageClass, nil first and then by
	// bytes, then by SizeBand ascending. Nil with a zero Split.
	//
	// The groups recombine to Totals: their entries add; their bytes and
	// allocated bytes add, nil when any group's is nil or the sum
	// overflows; and the newest change is the latest of theirs, nil when
	// any group's is nil.
	Groups []Group
	// Complete says every entry at or under the target was read: its own
	// entry's state is known (the root's, which is no entry, excepted) and
	// the inventory has no unlocated gap.
	Complete bool
}
