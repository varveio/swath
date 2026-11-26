// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"strconv"
	"unicode"

	"golang.org/x/text/width"
)

// Cells is s's width on a terminal, in columns: two for an East Asian
// wide or fullwidth character, none for a combining mark or a control,
// else one.
func Cells(s string) int {
	n := 0
	for _, r := range s {
		n += cell(r)
	}
	return n
}

// cell is r's width on a terminal (Cells).
func cell(r rune) int {
	switch {
	case unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf) || unicode.IsControl(r):
		return 0
	}
	switch width.LookupRune(r).Kind() {
	case width.EastAsianWide, width.EastAsianFullwidth:
		return 2
	}
	return 1
}

// Clip is name cut to at most room terminal columns (Cells), but never
// shorter than the ellipsis: the middle replaced by an ellipsis so that
// its end, an extension, stays. A name that fits is returned as it is.
func Clip(name string, room int) string {
	if Cells(name) <= room {
		return name
	}
	r := []rune(name)
	keep := max(room, 1) - 1 // the ellipsis takes one
	used, tail := 0, len(r)  // the end first, up to half the room
	for tail > 0 && used+cell(r[tail-1]) <= (keep+1)/2 {
		tail--
		used += cell(r[tail])
	}
	head := 0 // then the start, in what is left
	for head < tail && used+cell(r[head]) <= keep {
		used += cell(r[head])
		head++
	}
	return string(r[:head]) + "…" + string(r[tail:])
}

// iecUnits are the binary units sizes are given in.
var iecUnits = [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// Size is n bytes for people to read: bytes below 1 KiB, else at most
// three significant digits in binary units (5.88 MiB, 695 KiB, 0.98 MiB
// for 1000 KiB), as du -h does. It is the view's size column and every
// size swath prints on a terminal.
func Size(n int64) string { return string(AppendSize(nil, n)) }

// AppendSize appends Size(n) to b.
func AppendSize(b []byte, n int64) []byte {
	if n < 1024 {
		return append(strconv.AppendInt(b, n, 10), " B"...)
	}
	v, u := float64(n), 0
	for v >= 1024 && u < len(iecUnits)-1 {
		v /= 1024
		u++
	}
	if v >= 999.5 && u < len(iecUnits)-1 { // four digits: the next unit's 0.98 to 1.00
		v /= 1024
		u++
	}
	prec := 0
	switch {
	case v < 9.995:
		prec = 2
	case v < 99.95:
		prec = 1
	}
	return append(append(strconv.AppendFloat(b, v, 'f', prec, 64), ' '), iecUnits[u]...)
}

// Count is a count as the view's count column gives it: up to 999 as it
// is, else three significant digits in thousands (k), millions (M),
// billions (G) or more (T, P, E): 12.0k, 37.1M.
func Count(n int64) string {
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}
	const units = "kMGTPE"
	v, u := float64(n)/1000, 0
	for v >= 999.5 && u < len(units)-1 {
		v /= 1000
		u++
	}
	prec := 0
	switch {
	case v < 9.995:
		prec = 2
	case v < 99.95:
		prec = 1
	}
	return string(append(strconv.AppendFloat(nil, v, 'f', prec, 64), units[u]))
}

// Grouped is a count in full, its digits in groups of three (60,010), as
// a fold row's "(1,234 more)" and swath's tables give it.
func Grouped(n int64) string {
	var num [24]byte
	d := strconv.AppendInt(num[:0], n, 10)
	b := make([]byte, 0, len(d)+len(d)/3)
	if n < 0 {
		b, d = append(b, '-'), d[1:]
	}
	for i, c := range d {
		if i > 0 && (len(d)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, c)
	}
	return string(b)
}
