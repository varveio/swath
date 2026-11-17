// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"os"

	"golang.org/x/sys/unix"
)

// levelGetdents reads a level's directory entries; a test replaces it to
// make the filesystem leave them untyped.
var levelGetdents = unix.Getdents

// readLevel calls add with each name the directory fd holds and whether it
// is a directory, as getdents types it, until add fails. An entry the
// filesystem leaves untyped is statted relative to fd without following a
// link; one whose stat fails is no directory here, and its page gives it
// an unknown kind (levelEntries), so one entry never fails the level.
func readLevel(ctx context.Context, _ *os.File, fd int, add func(name string, dir bool) error) error {
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

		var addErr error
		err = parseDirents(buf[:n], func(_ uint64, typ uint8, name []byte) {
			if addErr == nil {
				addErr = add(string(name), typ == unix.DT_DIR || typ == unix.DT_UNKNOWN && isDirAt(fd, name))
			}
		})
		if err != nil {
			return readError(err)
		}
		if addErr != nil {
			return addErr
		}
	}
}

// isDirAt says whether name in the directory fd is a directory, not
// following a link: false when the stat fails.
func isDirAt(fd int, name []byte) bool {
	var s unix.Stat_t
	return unix.Fstatat(fd, string(name), &s, unix.AT_SYMLINK_NOFOLLOW) == nil && s.Mode&unix.S_IFMT == unix.S_IFDIR
}
