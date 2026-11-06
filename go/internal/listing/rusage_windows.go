// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package listing

import (
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// processMemoryCounters is PROCESS_MEMORY_COUNTERS (psapi.h).
type processMemoryCounters struct {
	cb                         uint32
	pageFaultCount             uint32
	peakWorkingSetSize         uintptr
	workingSetSize             uintptr
	quotaPeakPagedPoolUsage    uintptr
	quotaPagedPoolUsage        uintptr
	quotaPeakNonPagedPoolUsage uintptr
	quotaNonPagedPoolUsage     uintptr
	pagefileUsage              uintptr
	peakPagefileUsage          uintptr
}

// K32GetProcessMemoryInfo is kernel32's export of psapi's
// GetProcessMemoryInfo (Windows 7 and later).
var procGetProcessMemoryInfo = windows.NewLazySystemDLL("kernel32.dll").NewProc("K32GetProcessMemoryInfo")

// rusage is the process's CPU times (GetProcessTimes) and peak working
// set, Windows' peak resident set (GetProcessMemoryInfo).
func rusage() (user, sys time.Duration, peak int64, ok bool) {
	h := windows.CurrentProcess()
	var created, exited, kernel, usr windows.Filetime
	if windows.GetProcessTimes(h, &created, &exited, &kernel, &usr) != nil {
		return 0, 0, 0, false
	}
	var pmc processMemoryCounters
	pmc.cb = uint32(unsafe.Sizeof(pmc))
	if procGetProcessMemoryInfo.Find() != nil {
		return 0, 0, 0, false
	}
	if r, _, _ := procGetProcessMemoryInfo.Call(uintptr(h), uintptr(unsafe.Pointer(&pmc)), uintptr(pmc.cb)); r == 0 {
		return 0, 0, 0, false
	}
	return filetime(usr), filetime(kernel), int64(pmc.peakWorkingSetSize), true
}

// filetime is a FILETIME duration (100 ns units).
func filetime(f windows.Filetime) time.Duration {
	return time.Duration(int64(f.HighDateTime)<<32|int64(f.LowDateTime)) * 100
}
