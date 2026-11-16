// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package longpath

import "golang.org/x/sys/unix"

// Search opens a directory to resolve names in it. macOS has no O_PATH:
// the directory is opened for reading, which needs read permission on it.
const Search = unix.O_RDONLY

// statOnly opens what Stat reads without blocking on a FIFO; it needs read
// permission.
const statOnly = unix.O_RDONLY | unix.O_NONBLOCK
