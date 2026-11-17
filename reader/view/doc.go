// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package view answers the four questions of package listing (Page, Lookup,
// Walk and Summarize) from one opened swath dataset: an inventory. It plans
// and checks the questions, and SeekParquet executes them (Dataset.Page,
// Plan, Scan, List and AggregateRows); it reads a row's columns into the
// common listing.Entry, with nil for every value the row does not hold.
//
// The API is experimental: it may change in any release.
//
// # Evidence
//
// Every answer's Evidence comes from the manifest and is never wider than
// it: the origin is OriginInventory, the manifest digest is the SHA-256 of
// the manifest's bytes, the acquisition interval is the listing's start and
// finish and the publication time is the manifest's. Its Scope is the
// listing's prefix scope; a question outside it is unknown (ErrOutOfScope),
// never absent. Unlocated is the manifest's count of unreadable entries only
// when the listing reached the end of its scope and records no filter and no
// exclusion; otherwise it is nil. A key is proven absent only when
// Unlocated is 0, and only as of the listing.
//
// An inventory's own gaps are never located. When a bound of the request
// stops a Walk or a Summarize, the keys it did not reach come back as
// listing.UnreadInterval extents of class listing.UnreadBudget.
//
// # Keys
//
// An object store's prefix and subtree key may be any key prefix. A local
// one is empty, for the root, or a directory's key, ending in "/". A local
// inventory holds no row for its root, and the view invents none. A local
// key without a trailing "/" names a file or a directory: Lookup answers
// "a" with the directory "a/" when the inventory has it.
//
// # What the view checks
//
// Open reads nothing: it refuses an inventory of versions, whose keys
// repeat (listing.ErrUnsupported), and one whose manifest records no scope.
// It does not check every part's columns: an application that admits an
// inventory runs reader.Dataset.CheckSchema first. Every value the view
// reads is checked as it is read: a column a part lacks or holds with
// another physical type, a negative size, a local kind swath does not
// write, or a directory whose key does not end in "/" (and the reverse) is
// a *reader.SemanticError, and a key holding a NUL byte, which no listing
// key may hold, is listing.ErrInvalidPage. Summarize checks the values it
// sums, and the order of the keys it counts but not their form.
package view
