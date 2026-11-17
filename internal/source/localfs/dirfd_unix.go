// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/longpath"
	"github.com/varveio/swath/reader/listing"
)

// dirFD is an open directory shared by reference: the range that reads
// it, its chunks, and the queued ranges of its subdirectories, which are
// opened relative to it. Each holder releases its reference; the last
// closes the descriptor.
//
// A directory is kept (pooled set) when the walk's limit on kept
// descriptors allows it, the root always (keptFDs). One the limit refuses
// is still read through its descriptor, but only by the Expand that opened
// it: it is not shared among chunks or a continuation, and its
// subdirectories are opened from from, the kept directory it was itself
// opened from, below+1 names down (child); it holds a reference on from
// until it closes. So only kept descriptors outlive an Expand.
//
// The directory a caller holds open (OpenDir) is one with no counts (fds
// nil): it is opened from, never closed or released here.
type dirFD struct {
	fd     int
	f      *os.File // the portable engine's handle on fd, which closes it
	refs   atomic.Int32
	fds    *keptFDs // the walk's counts; nil for the caller's directory
	cost   int64    // the descriptors it is counted as holding (dirCost)
	pooled bool     // kept: counted in fds.open
	from   *dirFD
	below  int
	// seen is the directory's name set while a count enumerates it
	// (countSlab), handed on with its continuation.
	seen *nameSet
}

// release drops one reference to d; the last closes it and releases what
// it holds. A nil d, and the caller's directory, hold nothing.
func (d *dirFD) release() {
	if d == nil || d.fds == nil || d.refs.Add(-1) > 0 {
		return
	}
	if d.f != nil {
		d.f.Close()
	} else {
		unix.Close(d.fd)
	}
	d.fds.closed(d.cost)
	if d.pooled {
		d.fds.open.Add(-d.cost)
	}
	d.from.release()
}

// kept reports whether the limit keeps d, so queued work may hold it.
func (d *dirFD) kept() bool { return d.pooled }

// child is where a subdirectory of d is opened from, with a reference its
// range holds, and how many names down: d itself, one name down, or, when
// the limit refused to keep d, the directory d was opened from, one name
// further down than d.
func (d *dirFD) child() (*dirFD, int) {
	if d.kept() {
		d.refs.Add(1)
		return d, 1
	}
	d.from.refs.Add(1)
	return d.from, d.below + 1
}

// maxKept caps the directory descriptors a walk keeps (keepLimit).
const maxKept = 1024

// keepLimit is how many directory descriptors a walk keeps (keptFDs): a
// quarter of the process's soft descriptor limit, at most maxKept, so the
// rest stays the output's (encoders, spill readers) and the process's.
func keepLimit() int64 {
	var r unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &r); err != nil {
		return maxKept
	}
	return min(int64(r.Cur/4), maxKept)
}

// openRange opens r's directory to read it and returns it holding one
// reference, the caller's. A root opened by its path (New) is opened by
// it, as the run resolved it (a symbolic link followed; longpath past
// PATH_MAX). Every other directory, a held root's own (OpenDir) included,
// is opened relative to r.at, through the last r.below names of its key
// prefix, each with O_NOFOLLOW: never by its path, so a directory replaced
// by a symbolic link since its parent was listed is not followed, and the
// walk does not leave the root. With OneFilesystem each directory opened
// relative to another, intermediates included, is checked against the
// root's device.
//
// A directory it does not open is accounted for, reported unread, and ok
// is false: a symbolic link in its place as unsafe_dirs, a
// directory on another device as other_device_dirs (no error, as at a
// mount point), any other failure as open_errors; each by reason in
// dir_opens, and in long_paths when its path passes PATH_MAX. err is the
// context's error when the run stopped during a descent.
func (p *Provider) openRange(ctx context.Context, r Range, st *Stats) (d *dirFD, ok bool, err error) {
	long := len(r.path) >= unix.PathMax
	if long {
		st.LongPaths.attempt(len(longpath.Components(r.prefix)), len(r.path))
	}
	var fd int
	if r.at == nil {
		fd, err = longpath.OpenFD(p.root, unix.O_RDONLY|unix.O_DIRECTORY)
		if err == nil {
			p.keep.opened(1)
		}
	} else {
		if r.below > 1 && r.at != p.held {
			st.DirOpens.Redescents++
			if p.opt.MaxDescriptors > 0 {
				st.DirOpens.RedescentNames += int64(r.below)
				st.DirOpens.MaxRedescent = max(st.DirOpens.MaxRedescent, int64(r.below))
			}
		}
		fd, err = p.descend(ctx, r)
	}
	if err == nil {
		if long {
			st.LongPaths.Opened++
		}
		return p.adopt(fd, r, st), true, nil
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil, false, err
	}

	reason := openReason(err)
	if long {
		st.LongPaths.fail(reason)
	}
	if r.at != p.held {
		st.DirOpens.fail(reason)
	}
	p.unread(st, r.prefix, listing.UnreadSubtree, unreadClass(err))
	switch {
	case errors.Is(err, errOtherDevice):
		st.OtherDevice++
		return nil, false, nil
	case errors.Is(err, errSymlink):
		st.note(&st.Unsafe, r.path, err)
	default:
		st.note(&st.OpenErrors, r.path, err)
	}
	p.unreadableDir(r)
	return nil, false, nil
}

// descend opens r's directory from r.at (openRange): one name down, or,
// from an ancestor, one name at a time, each intermediate closed as the
// next opens and a stop checked for before each; an error then names the
// level that failed.
func (p *Provider) descend(ctx context.Context, r Range) (int, error) {
	if r.below == 1 {
		return p.openAt(r.at.fd, path.Base(r.prefix), true)
	}
	names := longpath.Components(r.prefix)
	at := r.at.fd
	dir := at
	for i := len(names) - r.below; i < len(names); i++ {
		if err := ctx.Err(); err != nil {
			p.closeTemporary(dir, at)
			return -1, err
		}
		fd, err := p.openAt(dir, names[i], i == len(names)-1)
		p.closeTemporary(dir, at)
		if err != nil {
			return -1, fmt.Errorf("level %d below the root: %w", i+1, err)
		}
		dir = fd
	}
	return dir, nil
}

// closeTemporary closes dir, an intermediate of a descent, unless it is
// at, the directory the descent started from.
func (p *Provider) closeTemporary(dir, at int) {
	if dir != at {
		unix.Close(dir)
		p.keep.closed(1)
	}
}

// openAt opens the directory name in dir, never following a symbolic
// link: to read it if last, else only to resolve names in it
// (longpath.Search). With OneFilesystem a directory on another device
// than the root's is closed and refused (errOtherDevice).
func (p *Provider) openAt(dir int, name string, last bool) (int, error) {
	mode := longpath.Search
	if last {
		mode = unix.O_RDONLY
	}
	fd, err := unix.Openat(dir, name, mode|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, classifyOpen(dir, name, err)
	}
	p.keep.opened(1)
	if !p.opt.OneFilesystem {
		return fd, nil
	}
	var s unix.Stat_t
	if err := unix.Fstat(fd, &s); err != nil || int64(s.Dev) != p.dev { //nolint:unconvert // Dev's type differs by platform
		unix.Close(fd)
		p.keep.closed(1)
		if err == nil {
			err = errOtherDevice
		}
		return -1, err
	}
	return fd, nil
}

// adopt holds the directory fd opened for r, with the caller's reference:
// the root always kept; another kept within the limit less its reserve
// or, at maxRedescent names or more below r.at, as a checkpoint within the
// whole limit (Provider.boundDescriptors), so that while the reserve
// lasts no directory is opened more than maxRedescent names below a kept
// one. The command reserves none: it keeps no checkpoint. One the limit
// refuses takes a reference on r.at for its subdirectories (child).
func (p *Provider) adopt(fd int, r Range, st *Stats) *dirFD {
	d := &dirFD{fd: fd, fds: &p.keep, cost: p.dirCost()}
	d.refs.Store(1)
	if r.at != p.held {
		st.DirOpens.Opened++
	}
	switch {
	case r.at == p.held:
		d.pooled = p.keep.take(keepRoot, d.cost)
	case p.keep.take(keepOrdinary, d.cost):
		d.pooled = true
	case p.keep.reserve > 0 && r.below >= maxRedescent && p.keep.take(keepCheckpoint, d.cost):
		d.pooled = true
		st.DirOpens.Checkpoints++
	}
	if d.pooled {
		return d
	}
	st.DirOpens.Refused++
	r.at.refs.Add(1)
	d.from, d.below = r.at, r.below
	return d
}

// streamFDs is how many descriptors a directory the portable engine reads
// holds: on macOS os.File.ReadDir reads through a directory stream on a
// duplicate of the descriptor (fdopendir), open until the file closes.
var streamFDs = func() int64 {
	if runtime.GOOS == "darwin" {
		return 2
	}
	return 1
}()

// dirCost is the descriptors a directory the walk opens is counted as
// holding (dirFD.cost): streamFDs for the portable engine when the walk
// bounds every descriptor (Options.MaxDescriptors), else one, as the
// command's limit on kept directories counts them.
func (p *Provider) dirCost() int64 {
	if !p.native && p.opt.MaxDescriptors > 0 {
		return streamFDs
	}
	return 1
}

// openPortable opens r's directory for the portable engine (openRange),
// with an os.File that reads its entries, counting the descriptor its
// reading may add (streamFDs).
func (p *Provider) openPortable(ctx context.Context, r Range, st *Stats) (*dirFD, bool, error) {
	d, ok, err := p.openRange(ctx, r, st)
	if ok {
		d.f = os.NewFile(uintptr(d.fd), r.path)
		p.keep.opened(d.cost - 1)
	}
	return d, ok, err
}

// classifyOpen tells a symbolic link apart from another non-directory when
// a no-follow directory open of name fails: O_NOFOLLOW refuses a link as
// ELOOP, but O_PATH with O_DIRECTORY as ENOTDIR.
func classifyOpen(dir int, name string, err error) error {
	if !errors.Is(err, unix.ENOTDIR) && !errors.Is(err, unix.ELOOP) {
		return err
	}
	var s unix.Stat_t
	if unix.Fstatat(dir, name, &s, unix.AT_SYMLINK_NOFOLLOW) == nil && s.Mode&unix.S_IFMT == unix.S_IFLNK {
		return errSymlink
	}
	return err
}

// openReason classifies a failed open for DirOpens.Failed and
// LongPaths.Failed.
func openReason(err error) string {
	switch {
	case errors.Is(err, errOtherDevice):
		return "other_device"
	case errors.Is(err, errSymlink):
		return "symlink"
	case errors.Is(err, unix.ENOENT):
		return "not_found"
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return "permission"
	case errors.Is(err, unix.ENOTDIR):
		return "not_directory"
	case errors.Is(err, unix.ENAMETOOLONG):
		return "name_too_long"
	}
	return "other"
}
