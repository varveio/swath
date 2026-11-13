// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !windows && !darwin

package localfs

import (
	"io/fs"
)

// statLinks: no tier reads a link count, device or inode here.
const statLinks = false

// fillSys: other platforms fill only what os.FileInfo exposes portably
// (kind, size, mtime, permission bits).
func fillSys(*Entry, fs.FileInfo) {}
