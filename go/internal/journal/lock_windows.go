// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package journal

import "os"

// Enforced reports whether the output lock and directory syncs hold here.
var Enforced = false // a variable for tests

// lockFile is not enforced on Windows yet.
func lockFile(*os.File) error { return nil }
