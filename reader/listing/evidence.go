// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "time"

// Origin says which backend answered. Its values are stable.
type Origin string

const (
	// OriginLive is an answer from a live source, read from the store or
	// the filesystem during the call.
	OriginLive Origin = "live"
	// OriginInventory is an answer from an opened inventory: the rows of
	// one immutable dataset.
	OriginInventory Origin = "inventory"
)

// StopReason says whether a call completed or which bound of its request
// stopped it. Reaching a bound is not an error: the call returns what it
// read. Any value other than StopCompleted, the empty one included, is an
// incomplete call. Its values are stable.
type StopReason string

const (
	// StopCompleted: the call did everything it was asked.
	StopCompleted StopReason = "completed"
	// StopEntryBudget: the call reached Budget.MaxEntries.
	StopEntryBudget StopReason = "entry_budget"
	// StopTimeBudget: the call reached its MaxDuration.
	StopTimeBudget StopReason = "time_budget"
	// StopHeldBudget: the call reached Budget.MaxHeldBytes.
	StopHeldBudget StopReason = "held_budget"
	// StopScanBudget: a page reached PageRequest.MaxScanned.
	StopScanBudget StopReason = "scan_budget"
)

// Evidence says where an answer came from and what it can prove. The zero
// Evidence proves nothing: no origin, an unknown count of unread entries,
// and no completed call.
type Evidence struct {
	// Origin is the backend that answered.
	Origin Origin
	// Manifest is the inventory's manifest digest; empty on a live answer.
	Manifest string
	// Started and Finished bound the interval over which the entries were
	// acquired: the call's own on a live answer, the listing's, across its
	// resumes, on an inventory's. A listing is not a point-in-time snapshot,
	// and neither time claims one: an entry that changed within the
	// interval may be in the answer as it was at any moment of it, or not
	// at all. Nil when an inventory's manifest does not record them.
	Started  *time.Time
	Finished *time.Time
	// Published is the inventory's publication time; nil on a live answer.
	Published *time.Time
	// Scope is the key prefix the answering source covers; empty for the
	// whole resource, as on every live answer. A question outside an
	// inventory's scope is unknown there, never absent (ErrOutOfScope).
	Scope []byte
	// Unlocated counts the entries in Scope the source may lack and whose
	// keys it did not record; nil when that count is not known. While it is
	// not 0, no key is proven absent and no walk is complete. A live answer
	// locates every gap as an Unread, and its Unlocated is 0. An
	// inventory's is a count only when its manifest proves the listing
	// reached the end of its scope and records no filter and no exclusion:
	// then it is the manifest's count of unreadable entries. Anything else
	// makes it nil, never a lower count: a listing that stopped early, a
	// filter (an include pattern, a size or time bound, a local option that
	// leaves entries out), an exclusion, or a coverage member the manifest
	// does not state. A filtered listing can be complete for its filters,
	// with nothing unreadable, and still lack entries that exist.
	Unlocated *int64
	// Stop says whether this call completed or which bound stopped it. It
	// is the query's completeness, not the source's coverage: a completed
	// call over an inventory with unlocated gaps is still partial.
	Stop StopReason
}

// UnreadScope says how much of the space at an Unread's key was not read.
// Its values are stable.
type UnreadScope string

const (
	// UnreadSubtree: nothing at or under Key was read, Key's own entry
	// included.
	UnreadSubtree UnreadScope = "subtree"
	// UnreadListing: Key's own enumeration did not end, so it may have
	// children the call never met. A child the call returned, and whose
	// own subtree it finished, was read.
	UnreadListing UnreadScope = "listing"
	// UnreadInterval: an object store's keys in (Key, Hi] were not read.
	// Key, exclusive, is empty when the interval starts at the beginning of
	// the call's scope; Hi, inclusive, is empty when it runs to the end.
	// Only keys within the call's scope are meant.
	UnreadInterval UnreadScope = "interval"
)

// UnreadClass says why an extent was not read. Its values are stable.
type UnreadClass string

const (
	// UnreadPermission: the source refused access (EACCES, EPERM, a
	// store's access denial).
	UnreadPermission UnreadClass = "permission"
	// UnreadIO: reading failed with an I/O error.
	UnreadIO UnreadClass = "io"
	// UnreadOtherDevice: a directory on another device than the source's
	// root. The directory is listed as a KindDir; its subtree is not
	// entered.
	UnreadOtherDevice UnreadClass = "other_device"
	// UnreadSymlinkAncestor: a directory on the way to Key was a symbolic
	// link when the walk opened it, and the walk follows no symlink.
	UnreadSymlinkAncestor UnreadClass = "symlink_ancestor"
	// UnreadTooDeep: Key lies deeper than the walk's limit on nested
	// directories.
	UnreadTooDeep UnreadClass = "too_deep"
	// UnreadNameTooLong: the operating system refused a name on the way
	// to Key as too long (ENAMETOOLONG).
	UnreadNameTooLong UnreadClass = "name_too_long"
	// UnreadUnstable: the directory changed while it was read in a way the
	// walk could not reconcile.
	UnreadUnstable UnreadClass = "unstable"
	// UnreadBudget: a bound of the request ended the reading of the
	// extent: the call stopped, and Evidence.Stop names the bound, or a
	// directory was too wide for Budget.MaxHeldBytes.
	UnreadBudget UnreadClass = "budget"
)

// Unread is an extent a call did not read: an entry or directory, or an
// object store's key interval. Everything in the call's scope that
// lies in no Unread extent was read. An entry at or under an UnreadSubtree
// key, or inside an UnreadInterval, may be missing from the answer; under
// an UnreadListing key, only the children the call did not return, and
// what lies under them, may be. Not following a symbolic link is policy,
// never an Unread. An inventory reports none of its listing's gaps as an
// Unread: they are unlocated (Evidence.Unlocated), and it never reports a
// root extent to stand for them.
type Unread struct {
	// Key is the entry or directory not read, or an interval's exclusive
	// low bound.
	Key []byte
	// Hi is an UnreadInterval's inclusive high bound; nil for any other
	// scope.
	Hi []byte
	// Scope says how much at Key was not read.
	Scope UnreadScope
	// Class says why.
	Class UnreadClass
}
