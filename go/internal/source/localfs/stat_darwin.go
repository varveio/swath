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
	fillStat(r, &s)
	if s.Btim.Sec != 0 || s.Btim.Nsec != 0 {
		r.Btime = schema.Micros(s.Btim.Unix())
		r.Present |= HasBtime
	}
	return nil
}

// fillSys fills the root device from os.FileInfo for the filesystem check.
func fillSys(r *Entry, fi fs.FileInfo) {
	s, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	r.Dev = int64(s.Dev)
	r.Present |= HasDev
}
