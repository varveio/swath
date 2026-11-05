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

// peakRSS returns VmHWM in bytes (0 if unavailable).
func peakRSS() int64 {
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

func cpuTimes() (user, sys time.Duration) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0, 0
	}
	return time.Duration(ru.Utime.Nano()), time.Duration(ru.Stime.Nano())
}
