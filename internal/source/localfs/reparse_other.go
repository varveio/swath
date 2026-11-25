// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package localfs

import "io/fs"

// reparseLinks: no entry is a reparse point here; a mount point is a
// directory.
const reparseLinks = false

func mountPoint(fs.FileInfo, string) (string, bool) { return "", false }
