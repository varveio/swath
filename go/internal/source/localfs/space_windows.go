// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package localfs

import (
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/windows"

	"github.com/varveio/swath/go/internal/source"
)

// measureSpace: GetDiskFreeSpaceExW takes a directory, so a link is
// resolved first and a file measures its directory; a UNC path needs a
// trailing backslash. The total and the available bytes are the
// caller's (within a quota), the free bytes the volume's.
func measureSpace(root string, file bool) *source.FilesystemSpace {
	dir, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil
	}
	if file {
		dir = filepath.Dir(dir)
	}
	if !strings.HasSuffix(dir, `\`) {
		dir += `\`
	}
	p, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return nil
	}
	var avail, total, free uint64
	if err := windows.GetDiskFreeSpaceEx(p, &avail, &total, &free); err != nil {
		return nil
	}
	return byteSpace(total, free, avail, time.Now())
}
