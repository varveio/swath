// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// Live is a directory the caller holds open, opened for one-level pages
// and lookups below it (OpenLive). It is a live.Store, safe for concurrent
// use. Each call opens what it reads relative to the held directory, one
// name at a time with O_NOFOLLOW, each checked against the held
// directory's device (resolve), and closes it before returning: nothing
// is held between calls, and the held directory is never read, closed or
// resolved by a path.
type Live struct {
	o        *Opened
	location string
}

var _ live.Store = (*Live)(nil)

// OpenLive opens root, a directory the caller opened (O_DIRECTORY, not
// following a symbolic link) and keeps open until the store is closed, as
// a live.Store. A root that is not a directory is listing.ErrNotDirectory.
func OpenLive(root *os.File) (live.Store, error) {
	var s unix.Stat_t
	if err := unix.Fstat(int(root.Fd()), &s); err != nil {
		return nil, classed(unreadClass(err), fmt.Errorf("the root: %w", err))
	}
	if s.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, fmt.Errorf("%w: the root is not a directory", listing.ErrNotDirectory)
	}
	o, err := OpenDir(root, "", Options{Fields: Stat})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", listing.ErrUnavailable, err)
	}
	// The root's device and inode, not its path: the caller may have given
	// none, and a path names whatever is there when it is resolved.
	return &Live{o: o, location: fmt.Sprintf("local\x00%d\x00%d", s.Dev, s.Ino)}, nil
}

// Location identifies the held directory by its device and inode
// (live.Store).
func (l *Live) Location() string { return l.location }

// Close releases nothing: the caller owns the held directory and closes
// it (live.Store).
func (l *Live) Close() error { return nil }

// List is one level of the directory at prefix, "" for the held one or a
// directory key below it, in byte order of keys, continuing after the key
// token names ("" starts the level) (live.Store). A child directory is a
// common prefix; any other child an entry of its kind, with what lstat
// gives of it. The directory's own entry is the level's first, on its
// first page, for a prefix below the held directory. Next is the last key
// the page read, and the next page holds the keys after it, not the names
// after a count of them, so that a directory that changes between pages
// repeats no key and skips no name that stayed.
//
// The directory is read whole to choose the page, again for each page,
// keeping only the names that may still be on it (levelNames): what a page
// holds follows max, not the directory's size. Only the page's entries
// are statted, relative to the directory's descriptor. One read directory
// is reported to ctx's objfile.Observer as one list request.
func (l *Live) List(ctx context.Context, prefix []byte, token string, max int) (*live.Level, error) {
	end := observe(ctx, objfile.ListRequest, string(prefix))
	lvl, err := l.list(ctx, string(prefix), token, max)
	end(err)
	return lvl, err
}

func (l *Live) list(ctx context.Context, prefix, token string, limit int) (*live.Level, error) {
	names, err := keyNames(prefix, true)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", listing.ErrInvalidRequest, err)
	}
	after := ""
	if token != "" {
		if !strings.HasPrefix(token, prefix) {
			return nil, fmt.Errorf("%w: its last key is outside the prefix", listing.ErrInvalidToken)
		}
		after = token[len(prefix):]
	}
	self := token == "" && prefix != ""

	fd, st, err := l.o.p.openLevel(ctx, names)
	if err != nil {
		return nil, failed(ctx, prefix, err)
	}
	f := os.NewFile(uintptr(fd), prefix) // it owns fd; its name, the caller's key, is all its errors can name
	defer f.Close()
	n := limit
	if self {
		n--
	}
	kids, more, err := levelNames(ctx, f, fd, after, n)
	if err != nil {
		return nil, failed(ctx, prefix, err)
	}

	lvl := &live.Level{}
	if self {
		var e Entry
		fillStat(&e, &st)
		lvl.Objects = append(lvl.Objects, entryOf(prefix, &e))
	}
	if more {
		lvl.Truncated, lvl.Next = true, prefix // a page of the directory's own entry alone
		if n > 0 {
			lvl.Next = prefix + kids[n-1].key
		}
	}
	if err := levelEntries(ctx, fd, prefix, kids, lvl); err != nil {
		return nil, failed(ctx, prefix, err)
	}
	return lvl, nil
}

// failed is a level's error: ctx's own once ctx has ended, else err,
// which is classed, naming the caller's prefix.
func failed(ctx context.Context, prefix string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w (prefix %q)", err, prefix)
}

// openLevel opens the directory names name below the held root to read
// it: its ancestors as resolve opens them, then the directory itself with
// O_RDONLY and O_NOFOLLOW, checked against the root's device; the held
// root as ".", so that the read has its own offset. It returns the
// descriptor and the directory's stat, or an error in the contract's
// class that says why it was not opened. At most two descriptors are open
// at once.
func (p *Provider) openLevel(ctx context.Context, names []string) (int, unix.Stat_t, error) {
	var s unix.Stat_t
	parent, name := names, "."
	if len(names) > 0 {
		parent, name = names[:len(names)-1], names[len(names)-1]
	}
	dir, stop, err := p.resolve(ctx, parent)
	switch {
	case err != nil:
		return -1, s, err
	case stop != nil:
		return -1, s, unreached(*stop)
	}

	fd, err := lookupOpenat(dir, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		err = notOpened(dir, name, len(names), err)
		p.closeResolved(dir)
		return -1, s, err
	}
	p.closeResolved(dir)

	if err := lookupFstat(fd, &s); err != nil {
		unix.Close(fd)
		return -1, s, classed(unreadClass(err), levelError(len(names), err))
	}
	if int64(s.Dev) != p.dev { //nolint:unconvert // Dev's type differs by platform
		unix.Close(fd)
		return -1, s, fmt.Errorf("%w: %w", listing.ErrOtherDevice, levelError(len(names), errOtherDevice))
	}
	return fd, s, nil
}

// unreached is the error of a level whose directory resolve did not reach:
// listing.ErrNotFound when nothing is there to reach, else the class of
// what stopped it.
func unreached(r LookupResult) error {
	if r.State == listing.LookupAbsent {
		return fmt.Errorf("%w: a directory on the way is not there", listing.ErrNotFound)
	}
	return classed(r.Class, r.Err)
}

// notOpened is the error of a level whose directory, name in dir at level
// below the root, failed to open with err: listing.ErrNotFound when
// nothing is there, listing.ErrNotDirectory when it is no directory (a
// symbolic link included: none is followed), else err's class. A
// no-follow directory open refuses a symbolic link and any other
// non-directory alike (classifyOpen), so the name says which.
func notOpened(dir int, name string, level int, err error) error {
	switch {
	case errors.Is(err, unix.ENOENT):
		return fmt.Errorf("%w: %w", listing.ErrNotFound, levelError(level, err))
	case !errors.Is(err, unix.ENOTDIR) && !errors.Is(err, unix.ELOOP):
		return classed(unreadClass(err), levelError(level, err))
	}

	var s unix.Stat_t
	serr := unix.Fstatat(dir, name, &s, unix.AT_SYMLINK_NOFOLLOW)
	switch {
	case errors.Is(serr, unix.ENOENT):
		return fmt.Errorf("%w: %w", listing.ErrNotFound, levelError(level, serr))
	case serr != nil:
		return classed(unreadClass(serr), levelError(level, serr))
	case s.Mode&unix.S_IFMT == unix.S_IFLNK:
		return fmt.Errorf("%w: %w", listing.ErrNotDirectory, levelError(level, errSymlink))
	case s.Mode&unix.S_IFMT == unix.S_IFDIR: // a directory again since the open
		return classed(listing.UnreadUnstable, levelError(level, err))
	}
	return fmt.Errorf("%w: %w", listing.ErrNotDirectory, levelError(level, err))
}

// levelError names where err happened by its depth below the root, never by
// a name.
func levelError(level int, err error) error {
	if level == 0 {
		return fmt.Errorf("the root: %w", err)
	}
	return fmt.Errorf("level %d below the root: %w", level, err)
}

// levelBatch is how many entries a level stats, and on macOS how many
// names it reads, between checks for a stop of ctx.
const levelBatch = 1024

// child is a name a level read: its key below the directory, with "/"
// after a directory's.
type child struct {
	key string
	dir bool
}

// levelNames reads f, the directory fd, whole (readLevel) and returns the
// first n children whose keys sort after after, sorted by key, each once (a
// name the directory returns twice in one read, renames moving entries in
// it, is kept once), and whether more children follow them. It holds at
// most 2n+1 names, whatever the directory's size: past 2n it keeps the
// first n+1 (trim), and from then on passes over a name that does not sort
// before the last of those (bound).
func levelNames(ctx context.Context, f *os.File, fd int, after string, n int) (kids []child, more bool, err error) {
	var key []byte // the name read, with "/" after a directory's
	bound := ""    // once trimmed to n+1 names, the last; keys are never empty
	trim := func() {
		slices.SortFunc(kids, func(a, b child) int { return strings.Compare(a.key, b.key) })
		kids = slices.CompactFunc(kids, func(a, b child) bool { return a.key == b.key })
		if len(kids) > n {
			kids, bound = kids[:n+1], kids[n].key
		}
	}
	err = readLevel(ctx, f, fd, func(name string, dir bool) {
		key = append(key[:0], name...)
		if dir {
			key = append(key, '/')
		}
		if string(key) <= after || bound != "" && string(key) >= bound {
			return
		}
		kids = append(kids, child{key: string(key), dir: dir})
		if len(kids)-n > n {
			trim()
		}
	})
	if err != nil {
		return nil, false, err
	}

	trim()
	if len(kids) > n {
		return kids[:n], true, nil
	}
	return kids, false, nil
}

// readError is a failed read of a level's directory, in its class.
func readError(err error) error {
	var errno unix.Errno
	if errors.As(err, &errno) {
		err = errno
	}
	return classed(unreadClass(err), fmt.Errorf("reading the directory: %w", err))
}

// levelEntries adds kids, children of the directory fd at prefix, to lvl:
// a directory as a common prefix, anything else as its entry, statted
// relative to fd without following a link. An entry gone since it was
// read is left out, and so is one that is a directory now, whose key would
// sort elsewhere; one whose stat fails is of unknown kind. A stop of ctx
// before the first stat, or between batches of them, fails the level.
func levelEntries(ctx context.Context, fd int, prefix string, kids []child, lvl *live.Level) error {
	for i, k := range kids {
		if i%levelBatch == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		key := prefix + k.key
		if k.dir {
			lvl.Prefixes = append(lvl.Prefixes, []byte(key))
			continue
		}
		var e Entry
		err := statAt(fd, k.key, Stat, &e)
		switch {
		case errors.Is(err, unix.ENOENT):
			continue
		case err != nil:
			e = Entry{Kind: KindUnknown}
		case e.Kind == KindDir:
			continue
		}
		lvl.Objects = append(lvl.Objects, entryOf(key, &e))
	}
	return nil
}

// Lookup says what is at exactly key below the held directory, as
// Opened.Lookup finds it (live.Store): a present entry with what lstat
// gives of it, absent, or unknown with the reason. A key of the wrong form
// is listing.ErrInvalidRequest. One lookup is reported to ctx's
// objfile.Observer as one metadata request.
func (l *Live) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	end := observe(ctx, objfile.MetadataRequest, string(key))
	r, err := l.lookup(ctx, string(key))
	if err == nil {
		end(r.Reason)
	} else {
		end(err)
	}
	return r, err
}

func (l *Live) lookup(ctx context.Context, key string) (listing.LookupResult, error) {
	r, err := l.o.Lookup(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			return listing.LookupResult{}, ctx.Err()
		}
		return listing.LookupResult{}, fmt.Errorf("%w: %w", listing.ErrInvalidRequest, err)
	}

	switch r.State {
	case listing.LookupPresent:
		e := entryOf(r.Row.Key, &r.Row.Data)
		return listing.LookupResult{State: listing.LookupPresent, Entry: &e}, nil
	case listing.LookupAbsent:
		return listing.LookupResult{State: listing.LookupAbsent}, nil
	}
	return listing.LookupResult{State: listing.LookupUnknown, Reason: reason(r.Class, r.Err)}, nil
}

// reason says why a lookup did not reach its key: in the class of the
// failure, or, for a symbolic link or another device on the way, an error
// of no class naming it (listing.LookupResult).
func reason(class listing.UnreadClass, err error) error {
	switch class {
	case listing.UnreadSymlinkAncestor, listing.UnreadOtherDevice:
		return fmt.Errorf("not reached: %w", err)
	}
	return classed(class, err)
}

// classed is err in the contract's class of the failure class names: a
// refusal (EACCES, EPERM) is listing.ErrAuth; another device than the
// root's listing.ErrOtherDevice; a name the system refuses as too long,
// and a symbolic link on the way, which is never followed, are
// listing.ErrUnsupported, which asking again does not change; an I/O
// error, and a tree that changed while it was resolved, are
// listing.ErrUnavailable.
func classed(class listing.UnreadClass, err error) error {
	c := listing.ErrUnavailable
	switch class {
	case listing.UnreadPermission:
		c = listing.ErrAuth
	case listing.UnreadOtherDevice:
		c = listing.ErrOtherDevice
	case listing.UnreadNameTooLong, listing.UnreadSymlinkAncestor:
		c = listing.ErrUnsupported
	}
	return fmt.Errorf("%w: %w", c, err)
}

// entryOf is the contract's entry of a row's data at key: its kind (a
// FIFO, socket or device is KindOther), and each value the stat read.
func entryOf(key string, d *Entry) listing.Entry {
	e := listing.Entry{Key: []byte(key), Kind: listingKinds[d.Kind]}
	if d.Has(HasSize) {
		e.Size = &d.Size
	}
	if d.Has(HasBlocks) {
		e.AllocatedBytes = &d.Blocks
	}
	if d.Has(HasMtime) {
		t := time.UnixMicro(d.Mtime).UTC()
		e.LastModified = &t
	}
	if d.Has(HasDev) {
		dev := uint64(d.Dev)
		e.Device = &dev
	}
	if d.Has(HasInode) {
		ino := uint64(d.Inode)
		e.Inode = &ino
	}
	if d.Has(HasNlink) {
		e.Links = &d.Nlink
	}
	return e
}

// listingKinds maps a kind to the contract's.
var listingKinds = [...]listing.Kind{
	KindUnknown:     listing.KindUnknown,
	KindFile:        listing.KindFile,
	KindDir:         listing.KindDir,
	KindSymlink:     listing.KindSymlink,
	KindFIFO:        listing.KindOther,
	KindSocket:      listing.KindOther,
	KindCharDevice:  listing.KindOther,
	KindBlockDevice: listing.KindOther,
}

// observe reports one request of kind on name, a key, to ctx's
// objfile.Observer, if it has one, and returns what ends it with the
// request's failure.
func observe(ctx context.Context, kind objfile.RequestKind, name string) func(error) {
	if obs := objfile.ObserverFromContext(ctx); obs != nil {
		if end := obs.Begin(objfile.Request{Kind: kind, Name: name}); end != nil {
			return func(err error) { end(objfile.RequestResult{Err: err}) }
		}
	}
	return func(error) {}
}
