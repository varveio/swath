// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package local opens a local directory, one the caller has already
// opened, as a live swath.Source: pages of one directory level, lookups of
// one key, walks and summaries of the tree below it. Linux and macOS only.
//
// The API is experimental: it may change in any release.
package local

import (
	"fmt"
	"os"

	"github.com/varveio/swath"
	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/source/localfs"
	"github.com/varveio/swath/reader/listing"
)

// Open opens root, a directory the caller opened, as a live source. The
// caller opens it with O_DIRECTORY and O_NOFOLLOW, owns it, and keeps it
// open until the source's Close returns; Close does not close it. The
// source never resolves root by a path: it opens every directory it reads
// relative to root's descriptor, one name at a time, never following a
// symbolic link, and refuses one on another device than root's. Keys are
// relative to root: "" pages root itself, "a/b/" the directory a/b below
// it.
//
// Page and Lookup answer as package listing states, in byte order of
// keys, which are any bytes but NUL. A child directory is a KindDir entry
// with its metadata, its key ending in "/"; a prefix's own directory comes
// back as the Self of its level's first page, beside Limit children. A
// page reads its whole directory, again for each page, and holds only the
// names that may still be on it, so its memory follows the request's
// Limit, not the directory's size: a request's MaxDuration bounds that
// work. A token resumes after the last key a page read, so a directory
// that changes between pages repeats no key and skips no name that
// stayed. A prefix that is not there is listing.ErrNotFound; one that names
// something else than a directory, a symbolic link included,
// listing.ErrNotDirectory, and so is one with a symbolic link on the way:
// a link is never followed, so what lies past it is not reached, never
// shown absent. A prefix on another device is listing.ErrOtherDevice.
// Lookup answers as the contract states for a local source; an unknown
// answer's Reason has the class a page through the same place fails with.
//
// Walk takes "" or a directory key, ending in "/", and refuses anything
// else as listing.ErrInvalidRequest: no literal prefix matching. It fails
// where a page of the same prefix fails, then hands on the directory's own
// entry, below the root, and lists what is under it with the parallel
// walker the swath command lists a tree with, each directory read relative
// to its parent, following no symbolic link and entering no other device.
// Its batches come in no key order, and a directory's entries unsorted. A
// directory it cannot open or read, an entry it cannot stat, and a
// directory on another device, listed and not entered, are handed to emit
// as an Unread as the walk meets them, and the walk goes on. A walk has no
// depth limit, but no key longer than 32 KiB: such an entry, a directory
// with its subtree, is an Unread of class listing.ErrUnsupported. It
// changes no process-wide setting. Summarize answers each target's state by a Lookup
// of its key, so a directory's subtree target is present, with the
// directory as its Self, even when empty, and walks each subtree target.
//
// A root that is not a directory is listing.ErrNotDirectory. On other
// systems than Linux and macOS, Open is listing.ErrUnsupported.
func Open(root *os.File) (*swath.Source, error) {
	if root == nil {
		return nil, fmt.Errorf("%w: no root", listing.ErrInvalidRequest)
	}

	st, err := localfs.OpenLive(root)
	if err != nil {
		return nil, err
	}
	return live.NewSource(st), nil
}
