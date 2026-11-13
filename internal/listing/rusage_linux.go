// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package listing

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// rusage is the process's CPU times and peak resident set: VmHWM, or
// getrusage's ru_maxrss (KiB) where /proc does not say.
func rusage() (user, sys time.Duration, peak int64, ok bool) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0, 0, 0, false
	}
	peak = vmHWM()
	if peak == 0 {
		peak = int64(ru.Maxrss) << 10
	}
	return time.Duration(ru.Utime.Nano()), time.Duration(ru.Stime.Nano()), peak, true
}

// vmHWM returns VmHWM in bytes (0 if unavailable).
func vmHWM() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "VmHWM:"); ok {
			kb, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v), "kB")), 10, 64)
			return kb << 10
		}
	}
	return 0
}

// residentBytes is the resident set now: /proc/self/statm's resident
// pages (the report's history).
func residentBytes() (int64, bool) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(f[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}
