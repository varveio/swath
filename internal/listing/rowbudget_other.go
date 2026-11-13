// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin && !windows

package listing

// physicalMemory is not read here: the default row budget falls back to
// DefaultRowBudget.
func physicalMemory() int64 { return 0 }

func cgroupMemory() int64 { return 0 }
