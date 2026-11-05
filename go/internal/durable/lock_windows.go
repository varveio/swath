// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package durable

import "os"

// lockFile is not enforced on Windows yet.
func lockFile(*os.File) error { return nil }
