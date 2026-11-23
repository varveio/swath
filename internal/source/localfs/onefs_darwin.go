// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import "golang.org/x/sys/unix"

const filesystemMechanism = "device_and_fsid"

// fstatfs reports the containing volume even where APFS firmlinks make
// fstat report the System volume's device for a directory on Data.
func filesystemID(fd int) ([2]int32, error) {
	var s unix.Statfs_t
	err := unix.Fstatfs(fd, &s)
	return s.Fsid.Val, err
}
