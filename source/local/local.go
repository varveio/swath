// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package local opens a local directory, one the caller has already
// opened, as a live source.Source: pages of one directory level, lookups of
// one key, walks and summaries of the tree below it. Linux and macOS only.
//
// The API is experimental: it may change in any release.
package local

import (
	"fmt"
	"os"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/source/localfs"
	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/source"
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
// The source answers as package listing states for a local source. A
// prefix's own directory comes back as the Self of its level's first page.
// A page's memory follows the request's Limit, not the directory's size,
// though each page reads its whole directory: a ctx deadline bounds that
// work. A token resumes after the last key a page read, so a
// directory that changes between pages repeats no key and skips no name
// that stayed. A prefix that names a symbolic link, or has one on the way,
// is listing.ErrNotDirectory: a link is never followed, so what lies past
// it is not reached, never shown absent. An unknown Lookup's Reason has
// the class a page through the same place fails with.
//
// A walk fails where a page of the same prefix fails. It has no depth
// limit, but no key longer than 32 KiB: such an entry, a directory with
// its subtree, is an Unread of class listing.ErrUnsupported. It changes no
// process-wide setting.
//
// A root that is not a directory is listing.ErrNotDirectory. On other
// systems than Linux and macOS, Open is listing.ErrUnsupported.
func Open(root *os.File) (*source.Source, error) {
	if root == nil {
		return nil, fmt.Errorf("%w: no root", listing.ErrInvalidRequest)
	}

	st, err := localfs.OpenLive(root)
	if err != nil {
		return nil, err
	}
	return (*source.Source)(live.NewSource(st, true)), nil
}
