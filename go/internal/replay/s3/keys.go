// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import "bytes"

// The 0xFF carry (increment the last byte below 0xFF, truncate after it)
// answers two different questions with the same bytes, and confusing them
// either drops keys or reopens a finished listing:
//
//   - PrefixUpper: the exclusive upper bound of every key starting with a
//     prefix. No finite bound (empty or all-0xFF prefix) means the range is
//     open: read to the end of the store.
//   - Successor: the least key after every key starting with a prefix, the
//     inclusive resume point past a rolled-up common prefix. No successor
//     (all-0xFF) means the listing is finished, never "start again".
//
// They are two functions with differently named results so a call site
// says which reading it wants.

// PrefixUpper returns the exclusive upper bound of the keys that start with
// prefix, or open=true when there is no finite bound.
func PrefixUpper(prefix []byte) (upper []byte, open bool) {
	if len(prefix) == 0 {
		return nil, true
	}
	c := carry(prefix)
	return c, c == nil
}

// Successor returns the least key greater than every key that starts with
// prefix, or end=true when nothing sorts after it (an empty or all-0xFF
// prefix).
func Successor(prefix []byte) (next []byte, end bool) {
	c := carry(prefix)
	return c, c == nil
}

func carry(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			out := bytes.Clone(prefix[:i+1])
			out[i]++
			return out
		}
	}
	return nil
}

// urlSafe is the set of bytes encoding-type=url leaves unescaped: letters,
// digits, '-', '_', '.', '/'. Everything else, including '~', '+' and every
// byte above 0x7F, is %XX with upper-case hex: the grammar S3 itself uses
// for ListObjectsV2 (TestPercentEncodeMatchesS3).
var urlSafe = func() (t [256]bool) {
	for c := 'A'; c <= 'Z'; c++ {
		t[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	t['-'], t['_'], t['.'], t['/'] = true, true, true, true
	return t
}()

const upperHex = "0123456789ABCDEF"

// AppendPercentEncoded appends raw in the encoding-type=url grammar.
func AppendPercentEncoded(dst, raw []byte) []byte {
	for _, b := range raw {
		if urlSafe[b] {
			dst = append(dst, b)
		} else {
			dst = append(dst, '%', upperHex[b>>4], upperHex[b&0x0f])
		}
	}
	return dst
}

// PercentEncode returns raw in the encoding-type=url grammar.
func PercentEncode(raw []byte) string {
	return string(AppendPercentEncoded(make([]byte, 0, len(raw)), raw))
}

// PercentDecode decodes %XX escapes (either hex case) to bytes and keeps
// every other byte, including a '%' not followed by two hex digits and '+',
// as it is. Query values keep their raw bytes this way: a key is bytes, not
// form text. Callers pass text from queryText, which is ASCII.
func PercentDecode(s string) []byte {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' && i+2 < len(s) {
			hi, lo := unhex(s[i+1]), unhex(s[i+2])
			if hi >= 0 && lo >= 0 {
				out = append(out, byte(hi<<4|lo))
				i += 2
				continue
			}
		}
		out = append(out, c)
	}
	return out
}

// queryText is the text the replay wire contract parses a raw query string
// as: the bytes are decoded as UTF-8, each maximal ill-formed subsequence
// becoming one U+FFFD (the WHATWG decoder), and then every non-ASCII code
// point, valid or replaced, becomes one '?'. Only %XX escapes carry
// non-ASCII bytes into a parameter; a raw é in the request line arrives as
// '?' (TestQueryTextMatchesTable).
func queryText(raw string) string {
	ascii := true
	for i := 0; i < len(raw); i++ {
		if raw[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return raw
	}
	out := make([]byte, 0, len(raw))
	needed, seen := 0, 0
	lower, upper := byte(0x80), byte(0xbf)
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if needed == 0 {
			switch {
			case b < 0x80:
				out = append(out, b)
			case b >= 0xc2 && b <= 0xdf:
				needed = 1
			case b >= 0xe0 && b <= 0xef:
				if b == 0xe0 {
					lower = 0xa0
				} else if b == 0xed {
					upper = 0x9f
				}
				needed = 2
			case b >= 0xf0 && b <= 0xf4:
				if b == 0xf0 {
					lower = 0x90
				} else if b == 0xf4 {
					upper = 0x8f
				}
				needed = 3
			default:
				out = append(out, '?')
			}
			continue
		}
		if b < lower || b > upper {
			// The sequence so far is one replacement; b starts afresh.
			out = append(out, '?')
			needed, seen, lower, upper = 0, 0, 0x80, 0xbf
			i--
			continue
		}
		lower, upper = 0x80, 0xbf
		seen++
		if seen == needed {
			out = append(out, '?')
			needed, seen = 0, 0
		}
	}
	if needed != 0 {
		out = append(out, '?')
	}
	return string(out)
}

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}
