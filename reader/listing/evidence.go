// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "time"

// Evidence says when an answer's entries were acquired and what it can
// prove. The zero Evidence proves nothing: an unknown count of unlocated
// entries.
type Evidence struct {
	// Started and Finished bound the interval over which the entries were
	// acquired: the call's own on a live answer, the listing's, across its
	// resumes, on an inventory's. A listing is not a point-in-time snapshot,
	// and neither time claims one: an entry that changed within the
	// interval may be in the answer as it was at any moment of it, or not
	// at all. Each is nil when the source does not know it, independently
	// of the other: an inventory's manifest may record when its listing
	// finished and not when it started, or neither.
	Started  *time.Time
	Finished *time.Time
	// Scope is the key prefix the answering source covers; empty for the
	// whole resource, as on every live answer. A question outside an
	// inventory's scope is unknown there, never absent (ErrOutOfScope).
	Scope []byte
	// Unlocated counts the entries in Scope the source may lack and whose
	// keys it did not record; nil when that count is not known. While it is
	// not 0, no key is proven absent and no walk is complete. A live answer
	// has 0: a walk hands on every place it could not read as an Unread;
	// a walk that returns an error, and an unknown Lookup, have nil. An
	// inventory's is a count only when its manifest proves the listing
	// reached the end of its scope and records no filter and no exclusion:
	// then it is the manifest's count of unreadable entries. Anything else
	// makes it nil, never a lower count: a listing that stopped early, a
	// filter (an include pattern, a size or time bound, a local option that
	// leaves entries out), an exclusion, or a coverage member the manifest
	// does not state. A filtered listing can be complete for its filters,
	// with nothing unreadable, and still lack entries that exist.
	Unlocated *int64
}

// Unread is a place a live walk could not read and went on past: a local
// directory it could not open or finish reading, or an entry it could not
// stat. What lies at or under Key may be missing from the walk; everything
// else in its scope that it finished was read. Not following a symbolic
// link is policy, never an Unread. An object store's walk has none: a
// listing error ends it. An inventory's has none either: its listing's
// gaps are unlocated (Evidence.Unlocated).
type Unread struct {
	// Key is the directory's or entry's key.
	Key []byte
	// Reason says why, an error matching one of the error classes:
	// ErrAuth for a refused access (EACCES, EPERM), ErrOtherDevice for a
	// directory on another device or macOS volume than the source's root, which is listed
	// as a KindDir and not entered, ErrNotDirectory for a symbolic link
	// found where a directory was opened, ErrUnsupported for a name the
	// system refuses as too long or a directory too large to read, and
	// ErrUnavailable for an I/O error or a directory that changed while
	// it was read.
	Reason error
}
