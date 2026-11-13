// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
)

func deltaFields(ints []int64, strs []string) DeltaFields {
	f := DeltaFields{Ints: make([]*int64, len(ints)), Strings: make([]*string, len(strs))}
	for i := range ints {
		f.Ints[i] = &ints[i]
	}
	for i := range strs {
		f.Strings[i] = &strs[i]
	}
	return f
}

func TestDeltaExtremeIntegers(t *testing.T) {
	values := []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64}
	for _, prev := range values {
		for _, cur := range values {
			p, c, got := []int64{prev}, []int64{cur}, []int64{0}
			encoded := AppendDelta(nil, deltaFields(p, nil), deltaFields(c, nil))
			d := NewDecoder(encoded)
			DecodeDelta(d, deltaFields(p, nil), deltaFields(got, nil))
			if d.Err() != nil || d.Len() != 0 || got[0] != cur {
				t.Fatalf("%d -> %d: %d, %v, %d left", prev, cur, got[0], d.Err(), d.Len())
			}
		}
	}
	// The two extreme transitions wrap to +1 and -1, respectively.
	for _, tt := range []struct {
		prev, cur int64
		want      []byte
	}{
		{math.MaxInt64, math.MinInt64, []byte{0, 2}},
		{math.MinInt64, math.MaxInt64, []byte{0, 1}},
	} {
		got := AppendDelta(nil, deltaFields([]int64{tt.prev}, nil), deltaFields([]int64{tt.cur}, nil))
		if !bytes.Equal(got, tt.want) {
			t.Fatalf("%d -> %d: %x, want %x", tt.prev, tt.cur, got, tt.want)
		}
	}
}

func TestDeltaRepeatMasks(t *testing.T) {
	for n := 0; n <= 8; n++ {
		for mask := 0; mask < 1<<n; mask++ {
			prev, cur, got := make([]string, n), make([]string, n), make([]string, n)
			var budget int64
			for i := range cur {
				prev[i] = fmt.Sprintf("previous-%d", i)
				if mask&(1<<i) != 0 {
					cur[i] = prev[i]
				} else {
					cur[i] = fmt.Sprintf("new-%d", i)
					budget += int64(len(cur[i]))
				}
			}
			src := AppendDelta(nil, deltaFields(nil, prev), deltaFields(nil, cur))
			if src[0] != byte(mask) {
				t.Fatalf("%d strings, mask %02x: flags %02x", n, mask, src[0])
			}
			d := NewOwningDecoder(src, budget)
			DecodeDelta(d, deltaFields(nil, prev), deltaFields(nil, got))
			if d.Err() != nil || d.Len() != 0 {
				t.Fatalf("%d strings, mask %02x: %v, %d left", n, mask, d.Err(), d.Len())
			}
			clear(src)
			if !slices.Equal(got, cur) {
				t.Fatalf("%d strings, mask %02x: %q, want %q", n, mask, got, cur)
			}
			if d.Alloc(1) != nil || !errors.Is(d.Err(), ErrDecodeCap) {
				t.Fatalf("%d strings, mask %02x: budget not exhausted: %v", n, mask, d.Err())
			}
		}
	}
}

func TestDeltaMalformed(t *testing.T) {
	for name, encoded := range map[string]string{
		"missing flags":           "",
		"unknown flags":           "02",
		"missing integer":         "00",
		"truncated integer":       "0080",
		"overflow integer":        "00ffffffffffffffffffff02",
		"missing string length":   "0000",
		"truncated string length": "000080",
		"overflow string length":  "0000ffffffffffffffffffff02",
		"short string":            "00000261",
	} {
		t.Run(name, func(t *testing.T) {
			src, err := hex.DecodeString(encoded)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range []*Decoder{NewDecoder(src), NewOwningDecoder(src, 100)} {
				DecodeDelta(d, deltaFields([]int64{0}, []string{""}), deltaFields([]int64{0}, []string{""}))
				if !errors.Is(d.Err(), ErrCodec) || d.Len() != 0 {
					t.Fatalf("%v, %d left", d.Err(), d.Len())
				}
			}
		})
	}
	// With no string fields, every flag bit is unknown.
	for bit := range 8 {
		d := NewDecoder([]byte{1 << bit})
		DecodeDelta(d, DeltaFields{}, DeltaFields{})
		if !errors.Is(d.Err(), ErrCodec) {
			t.Fatalf("flag %d: %v", bit, d.Err())
		}
	}
}

func TestDeltaOwningBudget(t *testing.T) {
	rows := [][]string{{"same", "old"}, {"same", "new"}, {"same", "new"}}
	var src []byte
	prev := []string{"", ""}
	for _, cur := range rows {
		src = AppendDelta(src, deltaFields(nil, prev), deltaFields(nil, cur))
		prev = cur
	}
	for _, budget := range []int64{9, 10} {
		encoded := bytes.Clone(src)
		d := NewOwningDecoder(encoded, budget)
		prev = []string{"", ""}
		var decoded [][]string
		for range rows {
			cur := make([]string, 2)
			DecodeDelta(d, deltaFields(nil, prev), deltaFields(nil, cur))
			decoded = append(decoded, cur)
			prev = cur
		}
		if budget == 9 {
			if !errors.Is(d.Err(), ErrDecodeCap) {
				t.Fatalf("short budget: %v", d.Err())
			}
			continue
		}
		if d.Err() != nil || d.Len() != 0 {
			t.Fatalf("exact budget: %v, %d left", d.Err(), d.Len())
		}
		clear(encoded)
		for i := range rows {
			if !slices.Equal(decoded[i], rows[i]) {
				t.Fatalf("row %d: %q, want %q", i, decoded[i], rows[i])
			}
		}
	}
}
