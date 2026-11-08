// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"slices"
	"unicode/utf16"
)

// CompareUTF16 orders two names as Azure lists them, from the definition:
// both are encoded to UTF-16 code units and the sequences compared
// lexicographically, a proper prefix first. It is written independently of
// the replay's byte-rank comparison (replay/azure.Compare) so that a fake
// sorted with it can check code that relies on that one. Defined for valid
// UTF-8; an invalid byte reads as U+FFFD.
func CompareUTF16(a, b string) int {
	return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
}
