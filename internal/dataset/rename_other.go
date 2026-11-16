// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package dataset

import "os"

// renameFile renames old to new, replacing new (an atomic replacement).
func renameFile(old, new string) error { return os.Rename(old, new) }
