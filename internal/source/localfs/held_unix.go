// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/longpath"
	"github.com/varveio/swath/reader/listing"
)

// OpenDir opens the directory root, which the caller opened (O_DIRECTORY,
// not following a symbolic link) and keeps open, the *os.File reachable,
// until the listing has returned and every range was expanded or
// disposed, to list the subtree of the directory at prefix: "" for root
// itself, or a directory's key below it ("a/b/"). Keys are relative to
// root. Nothing is resolved by a path: the listing opens every directory,
// prefix's included, relative to root's descriptor, one name at a time
// with O_NOFOLLOW, each checked against root's device (OneFilesystem is
// set, so opt needs a stat tier), and Lookup resolves a key the same way.
func OpenDir(root *os.File, prefix string, opt Options) (*Opened, error) {
	if _, err := keyNames(prefix, true); err != nil {
		return nil, err
	}
	var s unix.Stat_t
	if err := unix.Fstat(int(root.Fd()), &s); err != nil {
		return nil, fmt.Errorf("localfs: the root: %w", err)
	}
	switch {
	case s.Mode&unix.S_IFMT != unix.S_IFDIR:
		return nil, errors.New("localfs: the root is not a directory")
	case opt.Fields == Names:
		return nil, errors.New("localfs: a held root needs each directory's device: not with the names metadata tier")
	case opt.Fields == Statx && (opt.Portable || !nativeEngine):
		return nil, errors.New("the statx tier needs the Linux engine")
	}

	opt.OneFilesystem = true
	p := &Provider{
		root:     ".",
		opt:      opt,
		native:   nativeEngine && !opt.Portable,
		dev:      int64(s.Dev), //nolint:unconvert // Dev's type differs by platform
		held:     &dirFD{fd: int(root.Fd())},
		heldFile: root,
		prefix:   prefix,
	}
	p.keep.limit = keepLimit()
	b, err := bind.Provider[Range, Entry](p, &Schema)
	if err != nil {
		return nil, err
	}
	return &Opened{bound: b, p: p}, nil
}

// Lookup answers what is at exactly key below the root OpenDir holds: "a/b"
// names an entry of any kind, "a/b/" a directory only. Each directory on
// the way is opened relative to the one before, from the held root, with
// O_NOFOLLOW and checked against the root's device; the last name is
// statted relative to its directory at the listing's tier, never
// following a link. ENOENT or ENOTDIR at the key, or at a directory on the
// way whose name is no symbolic link, is absence. A symbolic link at the
// key is present, as itself. A symbolic link or another device on the way,
// EACCES, EPERM, ENAMETOOLONG and other errors leave the key unknown, not
// reached. A key that is not one, a root not held, and a stop of ctx are
// errors. A lookup holds at most two descriptors at once, outside a
// listing's limit (Options.MaxDescriptors).
func (o *Opened) Lookup(ctx context.Context, key string) (LookupResult, error) {
	p := o.p
	if p.held == nil {
		return LookupResult{}, errors.New("localfs: a lookup needs a held root (OpenDir)")
	}
	names, err := keyNames(key, false)
	if err != nil {
		return LookupResult{}, err
	}
	if len(names) == 0 {
		return LookupResult{}, errors.New("localfs: a lookup needs a key below the root")
	}

	dir := p.held.fd
	defer func() {
		if dir != p.held.fd {
			unix.Close(dir)
		}
	}()
	for i, name := range names[:len(names)-1] {
		if err := ctx.Err(); err != nil {
			return LookupResult{}, err
		}
		fd, err := unix.Openat(dir, name, longpath.Search|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return lookupAncestor(dir, name, i+1, err), nil
		}
		if dir != p.held.fd {
			unix.Close(dir)
		}
		dir = fd
		var s unix.Stat_t
		if err := unix.Fstat(fd, &s); err != nil {
			return lookupUnknown(unreadClass(err), err), nil
		}
		if int64(s.Dev) != p.dev { //nolint:unconvert // Dev's type differs by platform
			return lookupUnknown(listing.UnreadOtherDevice, fmt.Errorf("level %d below the root: %w", i+1, errOtherDevice)), nil
		}
	}
	if err := ctx.Err(); err != nil {
		return LookupResult{}, err
	}

	last := names[len(names)-1]
	var row Row
	if err := statAt(dir, last, p.opt.Fields, &row.Data); err != nil {
		if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) {
			return LookupResult{State: listing.LookupAbsent}, nil
		}
		return lookupUnknown(unreadClass(err), err), nil
	}
	if strings.HasSuffix(key, "/") && row.Data.Kind != KindDir {
		return LookupResult{State: listing.LookupAbsent}, nil
	}
	row.Key = strings.TrimSuffix(key, "/")
	if row.Data.Kind == KindDir {
		row.Key += "/"
	}
	if row.Data.Kind == KindSymlink && p.opt.LinkTarget {
		if t, err := readlinkAt(dir, last); err == nil {
			row.Data.LinkTarget, row.Data.Present = t, row.Data.Present|HasLink
		}
	}
	return LookupResult{State: listing.LookupPresent, Row: row}, nil
}

// lookupAncestor answers a lookup whose directory at level below the root,
// name in dir, failed to open with err: absent when nothing is there or it
// is no directory and no symbolic link, unknown otherwise.
func lookupAncestor(dir int, name string, level int, err error) LookupResult {
	at := func(err error) error { return fmt.Errorf("level %d below the root: %w", level, err) }
	switch {
	case errors.Is(err, unix.ENOENT):
		return LookupResult{State: listing.LookupAbsent}
	case !errors.Is(err, unix.ENOTDIR) && !errors.Is(err, unix.ELOOP):
		return lookupUnknown(unreadClass(err), at(err))
	}

	// A no-follow directory open refuses a symbolic link and any other
	// non-directory alike (classifyOpen): the name says which.
	var s unix.Stat_t
	serr := unix.Fstatat(dir, name, &s, unix.AT_SYMLINK_NOFOLLOW)
	switch {
	case errors.Is(serr, unix.ENOENT):
		return LookupResult{State: listing.LookupAbsent}
	case serr != nil:
		return lookupUnknown(unreadClass(serr), at(serr))
	case s.Mode&unix.S_IFMT == unix.S_IFLNK:
		return lookupUnknown(listing.UnreadSymlinkAncestor, at(errSymlink))
	case s.Mode&unix.S_IFMT == unix.S_IFDIR: // a directory again since the open
		return lookupUnknown(listing.UnreadUnstable, at(err))
	}
	return LookupResult{State: listing.LookupAbsent}
}

// lookupUnknown is a lookup that did not reach its key.
func lookupUnknown(class listing.UnreadClass, err error) LookupResult {
	return LookupResult{State: listing.LookupUnknown, Class: class, Err: err}
}
