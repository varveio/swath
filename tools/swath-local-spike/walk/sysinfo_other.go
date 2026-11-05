// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !windows

package walk

import (
	"io/fs"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// fillSys: other platforms (macOS, BSD) fill only what os.FileInfo exposes
// portably (kind, size, mtime, permission bits). Platform Stat_t layouts are
// future work.
func fillSys(*record.Row, fs.FileInfo) {}
