// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package walk

import (
	"io/fs"
	"syscall"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// fillSys adds the stat fields os.FileInfo hides behind Sys() (Linux).
func fillSys(r *record.Row, fi fs.FileInfo) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	r.UID = st.Uid
	r.GID = st.Gid
	r.Nlink = uint32(st.Nlink)
	r.Inode = int64(st.Ino)
	r.Dev = int64(st.Dev)
	r.Blocks = st.Blocks * 512
	r.Mtime = record.Micros(st.Mtim.Unix())
	r.Ctime = record.Micros(st.Ctim.Unix())
	r.Atime = record.Micros(st.Atim.Unix())
	r.Mode = st.Mode & 0o7777
	r.Present |= record.HasStat
}
