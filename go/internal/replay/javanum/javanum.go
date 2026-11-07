// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package javanum reads numbers with the grammar the replay wire contract
// and the serve flags accept: signed decimal integers whose digits may come
// from any Basic Multilingual Plane script, the floating-point grammar of
// the latency profile (decimal or hexadecimal, an optional d/f suffix,
// surrounding control characters ignored), and the saturating
// float-to-integer conversion the delay arithmetic relies on.
//
// The digit set is a table, not unicode.Nd: it is the contract's set (the
// BMP decimal digits of Unicode 16.0, TestDigitTable), and Go's own tables
// follow a different Unicode version on their own schedule. Digits outside
// the BMP are refused, because the contract reads a value one UTF-16 code
// unit at a time and a surrogate is not a digit.
package javanum

import (
	"math"
	"strconv"
)

// digitZeros are the code points of the zero of every BMP run of decimal
// digits; each run is ten code points, zero to nine.
var digitZeros = [...]rune{
	0x0030, 0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6,
	0x0C66, 0x0CE6, 0x0D66, 0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0,
	0x1810, 0x1946, 0x19D0, 0x1A80, 0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620,
	0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0, 0xFF10,
}

// Digit is the value of r as a decimal digit, or false.
func Digit(r rune) (int, bool) {
	if r >= '0' && r <= '9' {
		return int(r - '0'), true
	}
	if r < 0x80 || r > 0xFFFF {
		return 0, false
	}
	for _, z := range digitZeros {
		if r >= z && r < z+10 {
			return int(r - z), true
		}
	}
	return 0, false
}

// ParseInt reads a signed decimal integer that fits in bits (32 or 64)
// bits: an optional leading '+' or '-', then one or more digits (Digit),
// nothing else.
func ParseInt(s string, bits int) (int64, bool) {
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
	// Accumulate negatively, as the magnitude of the most negative value
	// is one more than the largest positive one.
	limit := -int64(1) << (bits - 1)
	if !neg {
		limit = -(int64(1)<<(bits-1) - 1)
	}
	var v int64
	for _, r := range s {
		d, ok := Digit(r)
		if !ok {
			return 0, false
		}
		if v < limit/10 {
			return 0, false
		}
		v *= 10
		if v < limit+int64(d) {
			return 0, false
		}
		v -= int64(d)
	}
	if neg {
		return v, true
	}
	return -v, true
}

// ParseDouble reads a floating-point number: after dropping leading and
// trailing characters at or below U+0020, an optional sign, then either
// decimal digits with an optional fraction and exponent, or a
// hexadecimal significand (0x...) with a mandatory binary exponent (p...),
// then an optional d, D, f or F (the value is read as a double either
// way), or NaN or Infinity spelled exactly so. A value too large for a
// double is an infinity, one too small is zero.
func ParseDouble(s string) (float64, bool) {
	lo, hi := 0, len(s)
	for lo < hi && s[lo] <= ' ' {
		lo++
	}
	for hi > lo && s[hi-1] <= ' ' {
		hi--
	}
	s = s[lo:hi]
	body := s
	if body != "" && (body[0] == '+' || body[0] == '-') {
		body = body[1:]
	}
	switch body {
	case "NaN":
		return math.NaN(), true
	case "Infinity":
		if s[0] == '-' {
			return math.Inf(-1), true
		}
		return math.Inf(1), true
	}
	if n := len(body); n > 0 {
		switch body[n-1] {
		case 'd', 'D', 'f', 'F':
			body = body[:n-1]
			s = s[:len(s)-1]
		}
	}
	if !wellFormed(body) {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if ne, ok := err.(*strconv.NumError); !ok || ne.Err != strconv.ErrRange {
			return 0, false
		}
	}
	return f, true
}

// wellFormed checks an unsigned, suffix-free number against the grammar,
// so strconv.ParseFloat (which also accepts underscores, inf and other
// spellings) only ever sees what the grammar admits.
func wellFormed(b string) bool {
	if len(b) > 2 && b[0] == '0' && (b[1] == 'x' || b[1] == 'X') {
		i, digits := 2, 0
		for i < len(b) && isHex(b[i]) {
			i, digits = i+1, digits+1
		}
		if i < len(b) && b[i] == '.' {
			i++
			for i < len(b) && isHex(b[i]) {
				i, digits = i+1, digits+1
			}
		}
		if digits == 0 || i >= len(b) || (b[i] != 'p' && b[i] != 'P') {
			return false
		}
		return exponent(b[i+1:])
	}
	i, digits := 0, 0
	for i < len(b) && isDec(b[i]) {
		i, digits = i+1, digits+1
	}
	if i < len(b) && b[i] == '.' {
		i++
		for i < len(b) && isDec(b[i]) {
			i, digits = i+1, digits+1
		}
	}
	if digits == 0 {
		return false
	}
	if i == len(b) {
		return true
	}
	if b[i] != 'e' && b[i] != 'E' {
		return false
	}
	return exponent(b[i+1:])
}

// exponent checks an optionally signed run of decimal digits.
func exponent(b string) bool {
	if b != "" && (b[0] == '+' || b[0] == '-') {
		b = b[1:]
	}
	if b == "" {
		return false
	}
	for i := 0; i < len(b); i++ {
		if !isDec(b[i]) {
			return false
		}
	}
	return true
}

func isDec(c byte) bool { return c >= '0' && c <= '9' }

func isHex(c byte) bool { return isDec(c) || (c|0x20 >= 'a' && c|0x20 <= 'f') }

// Saturate converts f to an int64 toward zero, clamping out-of-range
// values to the nearest bound and NaN to zero, where a plain Go conversion
// is implementation-defined.
func Saturate(f float64) int64 {
	switch {
	case f != f:
		return 0
	case f >= 0x1p63:
		return math.MaxInt64
	case f <= -0x1p63:
		return math.MinInt64
	}
	return int64(f)
}
