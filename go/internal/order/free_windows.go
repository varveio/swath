// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package order

import "golang.org/x/sys/windows"

// freeBytes is the space available to the caller (its quota included) on
// dir's volume; on error it reports unlimited (the cap and ENOSPC still hold).
func freeBytes(dir string) int64 {
	p, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 1<<63 - 1
	}
	var avail uint64
	if err := windows.GetDiskFreeSpaceEx(p, &avail, nil, nil); err != nil || avail > 1<<63-1 {
		return 1<<63 - 1
	}
	return int64(avail)
}
