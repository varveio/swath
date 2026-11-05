// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package proc

import "time"

// PeakRSS is not implemented off Linux.
func PeakRSS() int64 { return 0 }

// CPU is not implemented off Linux.
func CPU() (user, sys time.Duration) { return 0, 0 }
