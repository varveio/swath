// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !windows

package fs

import (
	"io/fs"

	"github.com/varveio/swath/go/internal/schema"
)

// fillSys: other platforms fill only what os.FileInfo exposes portably
// (kind, size, mtime, permission bits).
func fillSys(*schema.Row, fs.FileInfo) {}
