// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin && !windows

package listing

import "time"

// rusage is unavailable here: the report says so (rusage_available false)
// and leaves the CPU and peak RSS values out rather than reporting zero.
func rusage() (user, sys time.Duration, peak int64, ok bool) { return 0, 0, 0, false }
