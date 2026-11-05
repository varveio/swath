// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package pqout

import "unsafe"

// unsafeBytes views an immutable string as bytes without copying. parquet-go
// copies values into its column buffers before WriteRowValues returns.
func unsafeBytes(s string) []byte {
	if s == "" {
		return []byte{}
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
