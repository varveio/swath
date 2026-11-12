// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"slices"
	"testing"
)

// sameMultiset compares opens or reads without imposing an order on concurrent
// requests. Every occurrence counts, so extra or missing requests still fail.
func sameMultiset[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[T]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		if counts[v] == 0 {
			return false
		}
		counts[v]--
	}
	return true
}

func TestSameMultiset(t *testing.T) {
	t.Run("opens", func(t *testing.T) {
		a := []string{"part-1", "part-2", "part-1"}
		if !sameMultiset(a, []string{"part-2", "part-1", "part-1"}) {
			t.Error("reordered opens differ")
		}
		if sameMultiset(a, []string{"part-1", "part-2", "part-2"}) {
			t.Error("changed open multiplicities compare equal")
		}
	})
	first := sourceRead{"data/part-00001.parquet", 2821, 11}
	second := sourceRead{"data/part-00001.parquet", 2384, 21}
	baseline := []sourceRead{first, second, first}
	for _, tc := range []struct {
		name  string
		reads []sourceRead
		equal bool
	}{
		{"identical", []sourceRead{first, second, first}, true},
		{"reordered", []sourceRead{second, first, first}, true},
		{"missing duplicate", []sourceRead{first, second}, false},
		{"extra duplicate", []sourceRead{first, second, first, first}, false},
		{"changed multiplicities", []sourceRead{first, second, second}, false},
		{"changed path", []sourceRead{first, second, {"data/part-00002.parquet", first.off, first.len}}, false},
		{"changed offset", []sourceRead{first, second, {first.path, first.off + 1, first.len}}, false},
		{"changed length", []sourceRead{first, second, {first.path, first.off, first.len + 1}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeA, beforeB := slices.Clone(baseline), slices.Clone(tc.reads)
			if got := sameMultiset(baseline, tc.reads); got != tc.equal {
				t.Errorf("sameMultiset(%v, %v) = %t, want %t", baseline, tc.reads, got, tc.equal)
			}
			if !slices.Equal(baseline, beforeA) || !slices.Equal(tc.reads, beforeB) {
				t.Error("comparison modified its inputs")
			}
		})
	}
	if !sameMultiset([]sourceRead(nil), []sourceRead{}) {
		t.Error("empty traces differ")
	}
}
