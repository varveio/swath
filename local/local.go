// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package local opens a local directory, one the caller has already
// opened, as a live swath.Source: pages of one directory level and
// lookups of one key below it. Linux and macOS only.
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

// Options configure a local source. The zero value is ready to use.
type Options struct {
	// MaxHeldBytes bounds the memory one page holds. A page reads its
	// directory's names whole and sorts them to choose its entries, and
	// holds each name that sorts after where the page starts, priced at
	// its length plus 64 bytes; a directory whose names pass the bound
	// fails the page with ErrTooWide. Zero takes 64 MiB.
	MaxHeldBytes int64
}

// defaultMaxHeldBytes is Options.MaxHeldBytes's zero value: about
// 800,000 names of 20 bytes.
const defaultMaxHeldBytes = 64 << 20

// ErrTooWide is a page whose directory holds more names than
// Options.MaxHeldBytes allows it to sort. It matches
// listing.ErrUnsupported: asking again with the same options fails alike.
var ErrTooWide = localfs.ErrTooWide

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
// keys. A child directory is a KindPrefix entry; a prefix's own directory
// comes back as the Self of its level's first page. A page reads its whole
// directory, again for each page: a request's MaxDuration bounds that
// work, its MaxScanned does not, and Options.MaxHeldBytes bounds the
// names it holds. A token resumes after the last key a page read, so a
// directory that changes between pages repeats no key and skips no name
// that stayed. Names are bytes, but the source refuses a key that is not
// UTF-8, and a page holding such a name is listing.ErrInvalidPage. A
// prefix that is not there is listing.ErrNotFound; one that names
// something else than a directory, a symbolic link included,
// listing.ErrNotDirectory; one on another device listing.ErrOtherDevice;
// one with a symbolic link on the way, which is never followed,
// listing.ErrUnsupported. Lookup answers as the contract states for a
// local source.
//
// A root that is not a directory is listing.ErrNotDirectory. On other
// systems than Linux and macOS, Open is listing.ErrUnsupported.
func Open(root *os.File, opts Options) (*swath.Source, error) {
	switch {
	case root == nil:
		return nil, fmt.Errorf("%w: no root", listing.ErrInvalidRequest)
	case opts.MaxHeldBytes < 0:
		return nil, fmt.Errorf("%w: a negative MaxHeldBytes", listing.ErrInvalidRequest)
	case opts.MaxHeldBytes == 0:
		opts.MaxHeldBytes = defaultMaxHeldBytes
	}

	st, err := localfs.OpenLive(root, opts.MaxHeldBytes)
	if err != nil {
		return nil, err
	}
	return live.NewSource(st).(*swath.Source), nil
}
