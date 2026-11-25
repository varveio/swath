// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package view answers the questions of package listing (Page, Lookup,
// Walk and Summarize) from one opened swath dataset: an inventory. It plans
// and checks the questions, and SeekParquet executes them (Dataset.Page,
// Plan, Scan and FoldRows); it reads a row's columns into the
// common listing.Entry, with nil for every value the row does not hold.
// OpenDir opens a dataset directory for it in one call; Open takes a
// reader.Dataset the caller opened.
//
// The API is experimental: it may change in any release.
//
// # Evidence
//
// Every answer's Evidence comes from the manifest and is never wider than
// it: the acquisition interval is the listing's start and finish, each as
// the manifest records it, and its Scope is the listing's prefix scope; a
// question outside it is unknown (ErrOutOfScope), never absent. Unlocated
// is the manifest's count of unreadable entries only when the listing
// reached the end of its scope and records no filter and no exclusion;
// otherwise it is nil. A key is proven absent only when
// Unlocated is 0, never at or under a local entry of unknown kind
// (listing.KindUnknown), which may be a directory the listing did not
// read, never below a symbolic link, which no listing follows (unknown,
// with listing.ErrNotDirectory as the Reason), and only as of the listing.
//
// An inventory's own gaps are never located: a Walk hands on no
// listing.Unread, and a Summarize target is complete only when Unlocated
// is 0.
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
// write, or a directory whose key does not end in "/" (and the reverse)
// is a *reader.SemanticError, and it, a key holding a NUL byte, which no
// listing key may hold, and any data SeekParquet refuses are
// listing.ErrInvalidPage. Summarize checks the values it sums, and the
// order of the keys it counts but not their form.
//
// # Errors
//
// A call that fails returns an error of a class of package listing, with
// the cause wrapped beside it: data the reader refuses is
// listing.ErrInvalidPage, a refused access (fs.ErrPermission, a store's 401 or 403) listing.ErrAuth,
// a feature or limit SeekParquet does not support listing.ErrUnsupported,
// and any other failure to read the dataset, an I/O or network error
// past the storage's retries, listing.ErrUnavailable. A context's error
// is returned as it is, and so is an error of a Walk's emit.
package view
