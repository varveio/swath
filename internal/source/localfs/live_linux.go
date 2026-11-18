// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// levelGetdents reads a level's directory entries; a test replaces it to
// make the filesystem leave them untyped.
var levelGetdents = unix.Getdents

// readLevel calls add with each name the directory fd holds and whether it
// is a directory, as getdents types it. The name is a view of the read
// buffer, valid only during the call. An entry the filesystem leaves
// untyped is statted relative to fd without following a link; one whose
// stat fails is no directory here, and its page gives it an unknown kind
// (levelEntries), so one entry never fails the level.
func readLevel(ctx context.Context, _ *os.File, fd int, add func(name string, dir bool)) error {
	buf := make([]byte, direntBuffer)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := levelGetdents(fd, buf)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return readError(err)
		}
		if n <= 0 {
			return nil
		}

		err = parseDirents(buf[:n], func(_ uint64, typ uint8, name []byte) {
			add(unsafe.String(unsafe.SliceData(name), len(name)), typ == unix.DT_DIR || typ == unix.DT_UNKNOWN && isDirAt(fd, name))
		})
		if err != nil {
			return readError(err)
		}
	}
}

// isDirAt says whether name in the directory fd is a directory, not
// following a link: false when the stat fails.
func isDirAt(fd int, name []byte) bool {
	var s unix.Stat_t
	return unix.Fstatat(fd, string(name), &s, unix.AT_SYMLINK_NOFOLLOW) == nil && s.Mode&unix.S_IFMT == unix.S_IFDIR
}
