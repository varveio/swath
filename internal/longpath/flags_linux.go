// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package longpath

import "golang.org/x/sys/unix"

// Search opens a directory only to resolve names in it (O_PATH): no read
// permission on it is needed, and fstat works on the descriptor.
const Search = unix.O_PATH

// statOnly opens what Stat reads without permission on it, nor blocking
// on a FIFO.
const statOnly = unix.O_PATH
