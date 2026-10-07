//go:build !linux

// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import "time"

// This diagnostic reports process CPU only on the measured Linux platform.
func processCPUTime() time.Duration { return 0 }
