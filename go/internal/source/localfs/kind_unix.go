// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import "golang.org/x/sys/unix"

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
