// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package localfs

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"golang.org/x/sys/unix"
)

// TestSortRunsEqualsSerial: a directory's entries sorted in any number of
// concurrent runs, merged, come out in the key order a serial sort of the
// full keys gives: names that are prefixes of each other, names around '/'
// (a directory's suffix) in byte order, bytes that are not UTF-8, and run
// counts that leave an odd run out of a merge round.
func TestSortRunsEqualsSerial(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261108, 1))
	alphabet := []byte("a-./0z\xff\x01")
	for _, n := range []int{0, 1, 2, 3, 7, 100, 1000, 5000} {
		names := map[string]bool{}
		for len(names) < n {
			b := make([]byte, 1+rng.IntN(6))
			for i := range b {
				b[i] = alphabet[rng.IntN(len(alphabet))]
			}
			if slices.Contains(b, '/') { // a name never holds '/'
				continue
			}
			names[string(b)] = true
		}
		for _, prefix := range []string{"", "p/", "deeper/prefix/"} {
			for runs := 1; runs <= 9; runs++ {
				t.Run(fmt.Sprintf("n%d/%q/runs%d", n, prefix, runs), func(t *testing.T) {
					sc := &scratch{}
					want := make([]string, 0, n)
					for name := range names {
						typ := uint8(unix.DT_REG)
						if rng.IntN(3) == 0 {
							typ = unix.DT_DIR
						}
						sc.ents = append(sc.ents, entry{nameOff: uint32(len(sc.names)), nameLen: uint32(len(name)), typ: typ})
						sc.names = append(sc.names, name...)
						k := prefix + name
						if typ == unix.DT_DIR {
							k += "/"
						}
						want = append(want, k)
					}
					slices.Sort(want)
					buildKeys(prefix, sc)
					sortRuns(len(prefix), sc, runs)
					got := make([]string, len(sc.ents))
					for i := range sc.ents {
						got[i] = keyOf(sc.keys, &sc.ents[i])
					}
					if !slices.Equal(got, want) {
						t.Fatalf("order differs from the serial sort of the keys:\ngot  %q\nwant %q", got, want)
					}
				})
			}
		}
	}
}

// TestHugeDirectorySortsInParallel: a directory above parallelSortMin is
// sorted in parallel runs and presized, both counted, and its listing still
// equals the sorted oracle (TestListingEqualsSortedOracle covers the
// 5,000-entry directory, below both thresholds).
func TestHugeDirectorySortsInParallel(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 33,000 files")
	}
	root := t.TempDir()
	n := parallelSortMin + 232
	for i := range n {
		// Shuffled creation order, so the directory is not read back sorted.
		name := fmt.Sprintf("%s/f%06d", root, (i*7919)%n)
		fd, err := unix.Open(name, unix.O_CREAT|unix.O_WRONLY|unix.O_CLOEXEC, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		unix.Close(fd)
	}
	for _, fields := range []Fields{Names, Stat} {
		t.Run(fields.String(), func(t *testing.T) {
			rows, st := list(t, root, Options{Fields: fields}, 4, 1<<20)
			want := oracle(t, root, true)
			keys := make([]string, len(rows))
			for i := range rows {
				keys[i] = rows[i].Key
			}
			if !slices.Equal(keys, want) {
				t.Fatalf("sequence differs: got %d keys, want %d", len(keys), len(want))
			}
			if st.ParallelSorts != 1 || st.Presized != 1 {
				t.Fatalf("parallel_sorts %d presized %d: want 1 and 1", st.ParallelSorts, st.Presized)
			}
		})
	}
}
