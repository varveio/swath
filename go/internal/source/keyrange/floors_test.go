// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"math"
	"testing"

	"github.com/varveio/swath/go/internal/counters"
)

// TestFloorArithmetic pins the estimate and the tail floor (Java StealMath).
func TestFloorArithmetic(t *testing.T) {
	// Window (a, e]: 1,000 keys listed up to b (a quarter of the window)
	// extrapolate to 3,000 left. Byte space, not digits: decimal keys
	// over-state the remainder (Java's estimate does the same).
	if est := estRemaining("b", "a", "e", 1000); math.Abs(est-3000) > 1e-6 {
		t.Fatalf("est %v, want 3000", est)
	}
	lo, hi := "p/0000000", "p/0010000"
	if est := estRemaining("p/0001000", lo, hi, 1000); est < 100_000 {
		t.Fatalf("decimal keys: est %v, expected the byte-space over-statement", est)
	}
	// Nothing consumed: the remaining window fraction itself.
	if est := estRemaining(lo, lo, hi, 0); est <= 0 || est > 1 {
		t.Fatalf("unconsumed est %v", est)
	}
	// The estimate is linear in the keys listed.
	if a, b := estRemaining("p/0001000", lo, hi, 500), estRemaining("p/0001000", lo, hi, 1000); math.Abs(2*a-b) > 1e-6 {
		t.Fatalf("not linear: %v %v", a, b)
	}
	for _, c := range []struct {
		est, f, ratio float64
		below         bool
	}{
		{10_000, 0.5, math.Inf(1), false}, // 5,000 past the pivot
		{4_000, 0.5, math.Inf(1), false},  // exactly 2,000 is at the floor ... see next
		{3_999, 0.5, math.Inf(1), true},
		{10_000, 0.75, math.Inf(1), false}, // 2,500
		{10_000, 0.85, math.Inf(1), true},  // 1,500
		{10_000, 0.5, 0.1, true},           // thinning: credited 1/16 = 625
		{40_000, 0.5, 0.1, false},          // 2,500 even at the 1/16 reach
	} {
		got := tailBelowFloor(c.est, c.f, c.ratio)
		if c.est == 4_000 {
			// est·0.5 = 2,000 is at the floor: <= blocks.
			if !got {
				t.Fatalf("est 4000 f 0.5: at the floor must block")
			}
			continue
		}
		if got != c.below {
			t.Fatalf("tailBelowFloor(%v, %v, %v) = %v", c.est, c.f, c.ratio, got)
		}
	}
	if !belowRemainingFloor(4000) || belowRemainingFloor(4001) {
		t.Fatal("remaining floor is 4 pages, inclusive")
	}
}

// TestFloorsGateDecide drives decide on a bounded range of uniform keys with
// idle workers: a range with four pages or less left is not split, the
// tail floor refuses a pivot that leaves the upper child under two pages,
// and a thinning keyspace shrinks the credited tail. Off, both split.
func TestFloorsGateDecide(t *testing.T) {
	// Keys of three base-94 printable digits; the byte-space estimate reads
	// them about 256/94 times denser than they are (key(8000) leaves ~19,000).
	key := func(i int) string {
		return fmt.Sprintf("p/%c%c%c", 0x21+i/(94*94), 0x21+i/94%94, 0x21+i%94)
	}
	page := func(from, n int) []string {
		k := make([]string, n)
		for i := range k {
			k[i] = key(from + i + 1)
		}
		return k
	}
	type tc struct {
		name    string
		hi      string
		dens    float64 // 0: from the page itself
		want    bool
		counter counters.ID // -1: none
	}
	cases := []tc{
		{"far bound", key(8_000), 0, true, -1},
		{"under four pages left", key(2_300), 0, false, cSkippedRemainingFloor},
		{"thinning tail", key(8_000), 1e-9, false, cSkippedTailFloor},
	}
	for _, floors := range []bool{true, false} {
		for _, c := range cases {
			p, err := newTest(newSorted(nil, false, 1), Options{Workers: 64, Floors: floors})
			if err != nil {
				t.Fatal(err)
			}
			r := Range{lo: key(0), hi: c.hi, id: 1, born: fromSeed}
			st := &running{id: 1, keys: 1000}
			p.running[1] = st
			pg := page(0, 1000)
			cur := pg[len(pg)-1]
			st.dens.observe(pg[0], cur, r.lo, r.hi, len(pg))
			if c.dens > 0 {
				st.dens.ewma *= c.dens
			}
			_, _, ok := p.decide(st, r, pg, cur)
			want := c.want || !floors
			if ok != want {
				t.Fatalf("floors=%v %s: split=%v, want %v", floors, c.name, ok, want)
			}
			if floors && c.counter >= 0 && p.c.Load(c.counter) != 1 {
				t.Fatalf("%s: counter %d not counted", c.name, c.counter)
			}
		}
	}
}
