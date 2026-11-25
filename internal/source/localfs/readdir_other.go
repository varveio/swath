// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package localfs

import (
	"io/fs"
	"os"
)

// readDir reads the directory f whole (os.File.ReadDir). Windows reads it
// itself, to keep each entry's space on disk and file ID.
func readDir(f *os.File) ([]fs.DirEntry, error) { return f.ReadDir(-1) }

// fillRoot: a file root's lstat has every stat column here.
func fillRoot(string, *Entry) {}
