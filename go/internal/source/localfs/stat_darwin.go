// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"io/fs"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/go/internal/schema"
)

// statEntry fills r from one fstatat of d relative to its open directory,
// never following a symlink. os.DirEntry.Info on darwin is an lstat of the
// full path, which resolves every component again for each entry and
// leaves out the fields below that os.FileInfo does not expose.
func statEntry(dir *os.File, d fs.DirEntry, r *Entry) error {
	var s unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), d.Name(), &s, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	r.Kind = kindOfMode(uint32(s.Mode))
	r.Size = s.Size
	r.Mode = uint32(s.Mode) & 0o7777
	r.UID, r.GID, r.Nlink = s.Uid, s.Gid, uint32(s.Nlink)
	r.Inode, r.Dev, r.Blocks = int64(s.Ino), int64(s.Dev), s.Blocks*512
	r.Mtime = schema.Micros(s.Mtim.Unix())
	r.Ctime = schema.Micros(s.Ctim.Unix())
	r.Atime = schema.Micros(s.Atim.Unix())
	r.Present |= HasStat
	if s.Btim.Sec != 0 || s.Btim.Nsec != 0 {
		r.Btime = schema.Micros(s.Btim.Unix())
		r.Present |= HasBtime
	}
	return nil
}

// fillSys fills the stat fields an os.FileInfo carries on darwin, birth
// time included, as statEntry does.
func fillSys(r *Entry, fi fs.FileInfo) {
	s, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	r.UID, r.GID, r.Nlink = s.Uid, s.Gid, uint32(s.Nlink)
	r.Inode, r.Dev, r.Blocks = int64(s.Ino), int64(s.Dev), s.Blocks*512
	r.Mtime = schema.Micros(s.Mtimespec.Unix())
	r.Ctime = schema.Micros(s.Ctimespec.Unix())
	r.Atime = schema.Micros(s.Atimespec.Unix())
	r.Mode = uint32(s.Mode) & 0o7777
	r.Present |= HasStat
	if s.Birthtimespec.Sec != 0 || s.Birthtimespec.Nsec != 0 {
		r.Btime = schema.Micros(s.Birthtimespec.Unix())
		r.Present |= HasBtime
	}
}
