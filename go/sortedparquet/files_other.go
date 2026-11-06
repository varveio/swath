// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package sortedparquet

// DefaultMaxOpenFiles is 4,096 where the process has no descriptor limit
// to derive it from (Windows handles are not bounded that way).
func DefaultMaxOpenFiles() int { return 4096 }
