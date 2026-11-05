// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package listing

import "time"

func peakRSS() int64 { return 0 }

func cpuTimes() (user, sys time.Duration) { return 0, 0 }
