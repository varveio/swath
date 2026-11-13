// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package order

import (
	"fmt"
	"syscall"
)

// fsNames are the statfs(2) magic numbers of the filesystems a spill
// directory commonly sits on (linux/magic.h).
var fsNames = map[int64]string{
	0x01021994: "tmpfs",
	0x858458f6: "ramfs",
	0xef53:     "ext4", // ext2, ext3 and ext4 share it
	0x58465342: "xfs",
	0x9123683e: "btrfs",
	0x2fc12fc1: "zfs",
	0xf2f52010: "f2fs",
	0x794c7630: "overlay",
	0x6969:     "nfs",
	0xff534d42: "cifs",
	0x65735546: "fuse",
	0x5346544e: "ntfs",
}

func fsType(dir string) string {
	var st syscall.Statfs_t
	if syscall.Statfs(dir, &st) != nil {
		return ""
	}
	if n, ok := fsNames[int64(st.Type)]; ok { //nolint:unconvert // the field's type differs by architecture
		return n
	}
	return fmt.Sprintf("0x%x", st.Type)
}
