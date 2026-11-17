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
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/longpath"
)

// dirFD is an open directory shared by reference: the range that reads
// it, its chunks, and the queued ranges of its subdirectories, which are
// opened relative to it. Each holder releases its reference; the last
// closes the descriptor.
//
// A directory is kept (keep set) when the walk's limit on kept
// descriptors allows it, the root always (keptFDs). One the limit refuses
// is still read through its descriptor, but only by the Expand that opened
// it: it is not shared among chunks or a continuation, and its
// subdirectories are opened from from, the kept directory it was itself
// opened from, below+1 names down (child); it holds a reference on from
// until it closes. So only kept descriptors outlive an Expand.
type dirFD struct {
	fd    int
	f     *os.File // the portable engine's handle on fd, which closes it
	refs  atomic.Int32
	keep  *keptFDs
	from  *dirFD
	below int
}

// release drops one reference to d; the last closes it and releases what
// it holds. A nil d holds nothing.
func (d *dirFD) release() {
	if d == nil || d.refs.Add(-1) > 0 {
		return
	}
	if d.f != nil {
		d.f.Close()
	} else {
		unix.Close(d.fd)
	}
	if d.keep != nil {
		d.keep.open.Add(-1)
	}
	d.from.release()
}

// kept reports whether the limit keeps d, so queued work may hold it.
func (d *dirFD) kept() bool { return d.keep != nil }

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
// reference, the caller's. The root is opened by its path, as the run
// resolved it (a symbolic link followed; longpath past PATH_MAX). Every
// directory below it is opened relative to r.at, through the last r.below
// names of its key prefix, each with O_NOFOLLOW: never by its path, so a
// directory replaced by a symbolic link since its parent was listed is
// not followed, and the walk does not leave the root. With OneFilesystem
// each directory opened below the root, intermediates included, is
// checked against the root's device.
//
// A directory it does not open is accounted for and ok is false: a
// symbolic link in its place as unsafe_dirs, a directory on another device
// as other_device_dirs (no error, as at a mount point), any other failure
// as open_errors; each by reason in dir_opens, and in long_paths when its
// path passes PATH_MAX. err is the context's error when the run stopped
// during a descent.
func (p *Provider) openRange(ctx context.Context, r Range, st *Stats) (d *dirFD, ok bool, err error) {
	long := len(r.path) >= unix.PathMax
	if long {
		st.LongPaths.attempt(len(longpath.Components(r.prefix)), len(r.path))
	}
	var fd int
	if r.at == nil {
		fd, err = longpath.OpenFD(p.root, unix.O_RDONLY|unix.O_DIRECTORY)
	} else {
		if r.below > 1 {
			st.DirOpens.Redescents++
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
	if r.at != nil {
		st.DirOpens.fail(reason)
	}
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
			if dir != at {
				unix.Close(dir)
			}
			return -1, err
		}
		fd, err := p.openAt(dir, names[i], i == len(names)-1)
		if dir != at {
			unix.Close(dir)
		}
		if err != nil {
			return -1, fmt.Errorf("level %d below the root: %w", i+1, err)
		}
		dir = fd
	}
	return dir, nil
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
	if !p.opt.OneFilesystem {
		return fd, nil
	}
	var s unix.Stat_t
	if err := unix.Fstat(fd, &s); err != nil || int64(s.Dev) != p.dev { //nolint:unconvert // Dev's type differs by platform
		unix.Close(fd)
		if err == nil {
			err = errOtherDevice
		}
		return -1, err
	}
	return fd, nil
}

// adopt holds the directory fd opened for r, with the caller's reference:
// kept if the limit allows, the root always. One the limit refuses takes
// a reference on r.at for its subdirectories (child).
func (p *Provider) adopt(fd int, r Range, st *Stats) *dirFD {
	d := &dirFD{fd: fd}
	d.refs.Store(1)
	if r.at != nil {
		st.DirOpens.Opened++
	}
	if p.keep.take(r.at == nil) {
		d.keep = &p.keep
		return d
	}
	st.DirOpens.Refused++
	r.at.refs.Add(1)
	d.from, d.below = r.at, r.below
	return d
}

// openPortable opens r's directory for the portable engine (openRange),
// with an os.File that reads its entries.
func (p *Provider) openPortable(ctx context.Context, r Range, st *Stats) (*dirFD, bool, error) {
	d, ok, err := p.openRange(ctx, r, st)
	if ok {
		d.f = os.NewFile(uintptr(d.fd), r.path)
	}
	return d, ok, err
}

// errOtherDevice: a directory opened below the root is on another device
// than the root's (OneFilesystem).
var errOtherDevice = errors.New("on another device than the root")

// errSymlink: a name opened as a directory is a symbolic link, which the
// walk never follows.
var errSymlink = errors.New("a symbolic link, not followed")

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
