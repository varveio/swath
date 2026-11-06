// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"unicode"
	"unicode/utf8"
)

// The replay wire contract decodes request parameters and renders unencoded
// key bytes as text with a fixed set of rules: malformed UTF-8 becomes
// U+FFFD in a particular grouping, whitespace is a particular set, integers
// accept any Unicode decimal digit. A different decoder would render a
// different number of replacement characters for the same malformed key,
// and the bytes on the wire would differ. The helpers below implement those
// rules; TestDecodeTextMatchesTable pins them case by case.

const replacement = "�"

// decodeText returns raw as text: valid UTF-8 is returned unchanged, and each
// malformed sequence is replaced by U+FFFD. The grouping of malformed bytes
// into one replacement follows the replay wire contract: a truncated
// sequence followed by a non-continuation byte is one replacement, an
// encoded surrogate (ED A0..BF xx) is one replacement for all three bytes,
// and a truncated sequence at the very end of the input is one replacement
// that consumes what remains of it.
func decodeText(raw []byte) string {
	if utf8.Valid(raw) { // utf8.Valid also rejects encoded surrogates
		return string(raw)
	}
	out := make([]byte, 0, len(raw)+8)
	sp, sl := 0, len(raw)
	for sp < sl {
		b1 := raw[sp]
		sp++
		switch {
		case b1 < 0x80:
			out = append(out, b1)
		case b1>>5 == 0b110 && b1&0x1e != 0:
			if sp < sl {
				b2 := raw[sp]
				sp++
				if notContinuation(b2) {
					out = append(out, replacement...)
					sp--
				} else {
					out = append(out, b1, b2)
				}
				continue
			}
			out = append(out, replacement...)
			return string(out)
		case b1>>4 == 0b1110:
			if sp+1 < sl {
				b2, b3 := raw[sp], raw[sp+1]
				sp += 2
				if malformed3(b1, b2, b3) {
					out = append(out, replacement...)
					sp -= 3
					sp += malformed3Len(raw, sp)
				} else if c := rune(b1&0x0f)<<12 | rune(b2&0x3f)<<6 | rune(b3&0x3f); c >= 0xD800 && c <= 0xDFFF {
					out = append(out, replacement...)
				} else {
					out = append(out, b1, b2, b3)
				}
				continue
			}
			if sp < sl && malformed3Prefix(b1, raw[sp]) {
				out = append(out, replacement...)
				continue
			}
			out = append(out, replacement...)
			return string(out)
		case b1>>3 == 0b11110:
			if sp+2 < sl {
				b2, b3, b4 := raw[sp], raw[sp+1], raw[sp+2]
				sp += 3
				uc := rune(b1&0x07)<<18 | rune(b2&0x3f)<<12 | rune(b3&0x3f)<<6 | rune(b4&0x3f)
				if notContinuation(b2) || notContinuation(b3) || notContinuation(b4) || uc < 0x10000 || uc > 0x10FFFF {
					out = append(out, replacement...)
					sp -= 4
					sp += malformed4Len(raw, sp)
				} else {
					out = append(out, b1, b2, b3, b4)
				}
				continue
			}
			if b1 > 0xf4 || sp < sl && malformed4Prefix(b1, raw[sp]) {
				out = append(out, replacement...)
				continue
			}
			sp++
			out = append(out, replacement...)
			if sp < sl && notContinuation(raw[sp]) {
				continue
			}
			return string(out)
		default:
			out = append(out, replacement...)
		}
	}
	return string(out)
}

func notContinuation(b byte) bool { return b&0xc0 != 0x80 }

func malformed3(b1, b2, b3 byte) bool {
	return (b1 == 0xe0 && b2&0xe0 == 0x80) || notContinuation(b2) || notContinuation(b3)
}

func malformed3Prefix(b1, b2 byte) bool {
	return (b1 == 0xe0 && b2&0xe0 == 0x80) || notContinuation(b2)
}

func malformed3Len(raw []byte, sp int) int {
	b1, b2 := raw[sp], raw[sp+1]
	if (b1 == 0xe0 && b2&0xe0 == 0x80) || notContinuation(b2) {
		return 1
	}
	return 2
}

func malformed4Prefix(b1, b2 byte) bool {
	return (b1 == 0xf0 && (b2 < 0x90 || b2 > 0xbf)) || (b1 == 0xf4 && b2&0xf0 != 0x80) || notContinuation(b2)
}

func malformed4Len(raw []byte, sp int) int {
	b1, b2 := raw[sp], raw[sp+1]
	if b1 > 0xf4 || malformed4Prefix(b1, b2) {
		return 1
	}
	if notContinuation(raw[sp+2]) {
		return 2
	}
	return 3
}

// isBlank reports whether s is empty or only whitespace in the replay wire
// contract's sense: the Unicode space, line and paragraph separators except
// the no-break spaces (U+00A0, U+2007, U+202F), plus TAB, LF, VT, FF, CR
// and the four information separators U+001C..U+001F.
func isBlank(s string) bool {
	for _, r := range s {
		if !isSpace(r) {
			return false
		}
	}
	return true
}

func isSpace(r rune) bool {
	switch {
	case r >= '\t' && r <= '\r', r >= 0x1c && r <= 0x1f:
		return true
	case r == 0xa0 || r == 0x2007 || r == 0x202f:
		return false
	}
	return unicode.In(r, unicode.Zs, unicode.Zl, unicode.Zp)
}

// parseInt32 parses a signed decimal int32 the way the replay wire contract
// reads max-keys: an optional '+' or '-', then one or more decimal digits
// from any script (Unicode category Nd), nothing else, no overflow.
func parseInt32(s string) (int32, bool) {
	if s == "" {
		return 0, false
	}
	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return 0, false
	}
	var v int64
	for _, r := range s {
		d, ok := decimalDigit(r)
		if !ok {
			return 0, false
		}
		v = v*10 + int64(d)
		if v > 1<<31 {
			return 0, false
		}
	}
	if neg {
		v = -v
	}
	if v > 1<<31-1 || v < -1<<31 {
		return 0, false
	}
	return int32(v), true
}

// decimalDigit is the value of a decimal digit in any script. Every Nd
// range in the Unicode tables is a whole number of 0..9 runs starting at a
// zero, so the value is the offset into the range modulo ten.
func decimalDigit(r rune) (int, bool) {
	if r >= '0' && r <= '9' {
		return int(r - '0'), true
	}
	if r < 0x80 || !unicode.Is(unicode.Nd, r) {
		return 0, false
	}
	for _, rg := range unicode.Nd.R16 {
		if r >= rune(rg.Lo) && r <= rune(rg.Hi) {
			return int(r-rune(rg.Lo)) % 10, true
		}
	}
	for _, rg := range unicode.Nd.R32 {
		if r >= rune(rg.Lo) && r <= rune(rg.Hi) {
			return int(r-rune(rg.Lo)) % 10, true
		}
	}
	return 0, false
}
