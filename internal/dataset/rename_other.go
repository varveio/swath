// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package dataset

import "os"

// replaceFile renames tmp over path (an atomic replacement).
func replaceFile(tmp, path string) error { return os.Rename(tmp, path) }
