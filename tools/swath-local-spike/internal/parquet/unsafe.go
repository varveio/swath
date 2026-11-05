// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquet

import "unsafe"

// unsafeBytes views an immutable string as bytes without copying. parquet-go
// copies values into its column buffers before WriteRowValues returns;
// TestValuesAreCopied pins that contract.
func unsafeBytes(s string) []byte {
	if s == "" {
		return []byte{}
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
