// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import "unsafe"

// unsafeString views b; b must not change afterwards (a page arena).
func unsafeString(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }
