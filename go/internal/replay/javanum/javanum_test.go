// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package javanum

import (
	"math"
	"testing"
)

// TestDigitTable checks the table's shape and the contract's verdicts at
// its edges (the BMP runs of the contract's Unicode version; a digit
// outside the BMP is a surrogate pair, never a digit).
func TestDigitTable(t *testing.T) {
	for i, z := range digitZeros {
		if i > 0 && z < digitZeros[i-1]+10 {
			t.Errorf("run %#x overlaps or is out of order", z)
		}
		for k := rune(0); k < 10; k++ {
			if d, ok := Digit(z + k); !ok || d != int(k) {
				t.Errorf("Digit(%#x) = %d, %v", z+k, d, ok)
			}
		}
	}
	for _, r := range []rune{'/', ':', 0x065F, 0x066A, 0x07BF, 0x07CA, 0xFF0F, 0xFF1A, 0x1D7D9, 0x1D7CE, 0x104A0, 0x2070, 0x00B2, 0x2460} {
		if d, ok := Digit(r); ok {
			t.Errorf("Digit(%#x) = %d, want not a digit", r, d)
		}
	}
}

func TestParseInt(t *testing.T) {
	for _, c := range []struct {
		s    string
		bits int
		want int64
	}{
		{"0", 32, 0}, {"-0", 32, 0}, {"+7", 32, 7}, {"2147483647", 32, math.MaxInt32}, {"-2147483648", 32, math.MinInt32},
		{"١٢", 32, 12}, {"１", 32, 1}, {"߀", 32, 0}, {"0000000000000000000012", 32, 12},
		{"9223372036854775807", 64, math.MaxInt64}, {"-9223372036854775808", 64, math.MinInt64},
	} {
		if got, ok := ParseInt(c.s, c.bits); !ok || got != c.want {
			t.Errorf("ParseInt(%q, %d) = %d, %v; want %d", c.s, c.bits, got, ok, c.want)
		}
	}
	for _, c := range []struct {
		s    string
		bits int
	}{
		{"", 32}, {"+", 32}, {"-", 32}, {"abc", 32}, {"1.5", 32}, {" 5", 32}, {"5 ", 32}, {"2147483648", 32},
		{"-2147483649", 32}, {"99999999999999999999", 32}, {"0x10", 32}, {"1_000", 32}, {"--1", 32}, {"+-1", 32},
		{"\U0001D7D9", 32},  // a mathematical digit one outside the BMP: a surrogate pair
		{"1\U0001D7CE", 32}, // the same after a valid digit
		{"9223372036854775808", 64}, {"-9223372036854775809", 64},
	} {
		if v, ok := ParseInt(c.s, c.bits); ok {
			t.Errorf("ParseInt(%q, %d) = %d, want refused", c.s, c.bits, v)
		}
	}
}

// TestParseDouble holds the grammar's verdicts, each one the contract's
// own answer for the same text.
func TestParseDouble(t *testing.T) {
	for s, want := range map[string]float64{
		"0x1p0": 1, "0x1.8p1": 3, "0x.8p1": 1, "0x1p0d": 1, "1.5f": 1.5, "1.5d": 1.5, " 2 ": 2, ".5": 0.5,
		"5.": 5, "+1": 1, "1.0e+2F": 100, "\t3\x01": 3, "1e-400": 0, "6.9": 6.9, "250": 250,
	} {
		if got, ok := ParseDouble(s); !ok || got != want {
			t.Errorf("ParseDouble(%q) = %v, %v; want %v", s, got, ok, want)
		}
	}
	if got, ok := ParseDouble("1e400"); !ok || !math.IsInf(got, 1) {
		t.Errorf("ParseDouble(1e400) = %v, %v", got, ok)
	}
	if got, ok := ParseDouble("-0"); !ok || got != 0 || !math.Signbit(got) {
		t.Errorf("ParseDouble(-0) = %v, %v", got, ok)
	}
	if got, ok := ParseDouble("NaN"); !ok || !math.IsNaN(got) {
		t.Errorf("ParseDouble(NaN) = %v, %v", got, ok)
	}
	for _, s := range []string{"", "1_0", "0x1", "nan", "infinity", "inf", "1e", "e1", " 1", ".", "0x", "0xp1",
		"1.2.3", "1e+", "0x1p", "1ff", "1 2", "+-1", "0b1", "1e1.5"} {
		if v, ok := ParseDouble(s); ok {
			t.Errorf("ParseDouble(%q) = %v, want refused", s, v)
		}
	}
}

func TestSaturate(t *testing.T) {
	for f, want := range map[float64]int64{
		9.3e18: math.MaxInt64, -9.3e18: math.MinInt64, 1e300: math.MaxInt64, math.Inf(1): math.MaxInt64,
		math.Inf(-1): math.MinInt64, 0x1p63: math.MaxInt64, 1.9: 1, -1.9: -1, 0: 0,
	} {
		if got := Saturate(f); got != want {
			t.Errorf("Saturate(%v) = %d, want %d", f, got, want)
		}
	}
	if got := Saturate(math.NaN()); got != 0 {
		t.Errorf("Saturate(NaN) = %d", got)
	}
}
