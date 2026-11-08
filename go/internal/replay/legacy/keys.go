// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import "bytes"

// Successor returns the least key above every key that begins with prefix:
// prefix without its trailing 0xFF bytes, with its last byte incremented. A
// prefix that is empty or only 0xFF bytes has none (ok is false): every key
// at or after it begins with it, so its range is unbounded above.
func Successor(prefix []byte) (next []byte, ok bool) {
	last := len(prefix) - 1
	for last >= 0 && prefix[last] == 0xff {
		last--
	}
	if last < 0 {
		return nil, false
	}
	next = append([]byte(nil), prefix[:last+1]...)
	next[last]++
	return next, true
}

// CommonPrefix returns the common prefix key rolls up to under prefix with
// the delimiter byte delim: key up to and including the first delim at or
// after len(prefix), or nil when there is none (key is then an object
// directly under prefix). It does not check that key begins with prefix:
// callers read only keys inside the prefix's range.
func CommonPrefix(key, prefix []byte, delim byte) []byte {
	if len(prefix) > len(key) {
		return nil
	}
	if i := bytes.IndexByte(key[len(prefix):], delim); i >= 0 {
		return key[:len(prefix)+i+1]
	}
	return nil
}
