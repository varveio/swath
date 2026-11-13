// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/schema"
)

func kindOfMode(m uint32) Kind {
	switch m & unix.S_IFMT {
	case unix.S_IFREG:
		return KindFile
	case unix.S_IFDIR:
		return KindDir
	case unix.S_IFLNK:
		return KindSymlink
	case unix.S_IFIFO:
		return KindFIFO
	case unix.S_IFSOCK:
		return KindSocket
	case unix.S_IFCHR:
		return KindCharDevice
	case unix.S_IFBLK:
		return KindBlockDevice
	}
	return KindUnknown
}

func fillStat(r *Entry, s *unix.Stat_t) {
	r.Kind = kindOfMode(uint32(s.Mode))
	r.Size = s.Size
	r.Mode = uint32(s.Mode) & 0o7777
	r.UID, r.GID, r.Nlink = s.Uid, s.Gid, uint32(s.Nlink)
	r.Inode, r.Dev, r.Blocks = int64(s.Ino), int64(s.Dev), s.Blocks*512
	r.Mtime = schema.Micros(s.Mtim.Unix())
	r.Ctime = schema.Micros(s.Ctim.Unix())
	r.Atime = schema.Micros(s.Atim.Unix())
	r.Present |= HasStat
}
