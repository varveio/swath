// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import "unicode/utf8"

// requestLineText is the text the replay wire contract reads the Azure
// route's raw query as: UTF-8, each maximal ill-formed subsequence
// replaced by one U+FFFD (the WHATWG decoder's grouping, which differs
// from ranging over a Go string: a truncated four-byte sequence is one
// replacement, not three). Valid non-ASCII characters pass through, so a
// raw é in the request line is é (TestRequestLineText).
func requestLineText(raw string) string {
	if utf8.ValidString(raw) {
		return raw
	}
	out := make([]byte, 0, len(raw)+8)
	needed, seen, start := 0, 0, 0
	lower, upper := byte(0x80), byte(0xbf)
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if needed == 0 {
			switch {
			case b < 0x80:
				out = append(out, b)
			case b >= 0xc2 && b <= 0xdf:
				needed, start = 1, i
			case b >= 0xe0 && b <= 0xef:
				if b == 0xe0 {
					lower = 0xa0
				} else if b == 0xed {
					upper = 0x9f
				}
				needed, start = 2, i
			case b >= 0xf0 && b <= 0xf4:
				if b == 0xf0 {
					lower = 0x90
				} else if b == 0xf4 {
					upper = 0x8f
				}
				needed, start = 3, i
			default:
				out = append(out, "\ufffd"...)
			}
			continue
		}
		if b < lower || b > upper {
			// The sequence so far is one replacement; b starts afresh.
			out = append(out, "\ufffd"...)
			needed, seen, lower, upper = 0, 0, 0x80, 0xbf
			i--
			continue
		}
		lower, upper = 0x80, 0xbf
		seen++
		if seen == needed {
			out = append(out, raw[start:i+1]...)
			needed, seen = 0, 0
		}
	}
	if needed != 0 {
		out = append(out, "\ufffd"...)
	}
	return string(out)
}
