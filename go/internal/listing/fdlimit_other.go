// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package listing

// fdLimit is the process's soft descriptor limit (0: unknown).
func fdLimit() int64 { return 0 }
