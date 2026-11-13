// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "golang.org/x/sys/unix"

// physicalMemory is the host's RAM (hw.memsize), 0 when unknown.
func physicalMemory() int64 {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return clampMemory(n)
}

// cgroupMemory: macOS has no cgroups.
func cgroupMemory() int64 { return 0 }
