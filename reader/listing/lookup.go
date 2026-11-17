// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "strconv"

// LookupState is what an answer says about the entry at exactly one key.
// The zero value is LookupUnknown, so a state never set reads as unknown,
// never as absent. The numeric values are not stable: record a state by
// its String form.
type LookupState uint8

const (
	// LookupUnknown: the answer proves neither presence nor absence.
	LookupUnknown LookupState = iota
	// LookupPresent: an entry is at the key.
	LookupPresent
	// LookupAbsent: no entry is at the key, proven. A live source proves
	// it only from an answer that rules the key out, such as a missing
	// object in a bucket that exists, or ENOENT or ENOTDIR on the way to a
	// local key. An inventory proves it only inside its scope, from a
	// listing with no unlocated gaps, and only as of that listing.
	LookupAbsent
)

var lookupStateNames = [...]string{
	LookupUnknown: "unknown",
	LookupPresent: "present",
	LookupAbsent:  "absent",
}

// String returns the state's name: unknown, present or absent;
// LookupState(N) for a value that is no state.
func (s LookupState) String() string {
	if int(s) < len(lookupStateNames) {
		return lookupStateNames[s]
	}
	return "LookupState(" + strconv.Itoa(int(s)) + ")"
}

// LookupResult says what is at exactly one key. Absence is never a
// fallback: an answer that cannot prove the key absent is unknown, with
// the reason.
type LookupResult struct {
	// State is present, absent or unknown.
	State LookupState
	// Entry is the entry at the key when State is LookupPresent; nil
	// otherwise. A local key whose last component is a symbolic link is
	// present, as a KindSymlink.
	Entry *Entry
	// Reason says why State is LookupUnknown: an error matching one of
	// the error classes, such as ErrAuth for an access denial, which S3
	// returns for a missing key without list permission, or ErrOutOfScope
	// for a key outside an inventory's scope. Nil when Evidence alone
	// explains it (an inventory with unlocated gaps), and on any other
	// state.
	Reason error
	// Evidence says where the answer came from.
	Evidence Evidence
}
