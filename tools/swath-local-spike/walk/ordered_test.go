// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package walk

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// orderTree has, at the same level, the tricky names as files in one
// directory and as directories in another (a name cannot be both in one
// directory), plus bytes above 0x7f and nested content. Byte order of the
// separators: '-' 0x2d < '.' 0x2e < '/' 0x2f < '0' 0x30 < 'b' 0x62 < 0x7f < 0xc3 (é) < 0xe6 (日).
func orderTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	tricky := []string{"a-b", "a.b", "a", "a0", "ab", "é", "日本", "z", "~", "\u007f", "a b"}
	mk := func(p string, dir bool) {
		full := filepath.Join(root, filepath.FromSlash(p))
		var err error
		if dir {
			err = os.MkdirAll(full, 0o755)
		} else {
			err = os.WriteFile(full, []byte("x"), 0o644)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// Level 1: odd-indexed names as directories, even-indexed as files...
	for i, n := range tricky {
		mk(n, i%2 == 1)
	}
	// ...and the opposite roles one level down, in "mixed".
	mk("mixed", true)
	for i, n := range tricky {
		mk("mixed/"+n, i%2 == 0)
		if i%2 == 0 {
			for _, m := range tricky {
				mk("mixed/"+n+"/"+m, false)
			}
			mk("mixed/"+n+"/sub/deeper", true)
			mk("mixed/"+n+"/sub/deeper/leaf", false)
		}
	}
	for i, n := range tricky {
		if i%2 == 1 { // directories at level 1: fill them
			for _, m := range tricky {
				mk(n+"/"+m, false)
			}
			mk(n+"/a.c", true)
			mk(n+"/a.c/x", false)
		}
	}
	// Empty directories at several positions.
	mk("mixed/empty", true)
	mk("a-c", true)
	mk("wide", true)
	// A wider directory to make workers run ahead.
	for i := 0; i < 300; i++ {
		mk("wide/f"+strings.Repeat("x", i%7)+string(rune('a'+i%26))+itoa(i), false)
	}
	return root
}

type seqSink struct {
	mu   sync.Mutex
	keys []string
}

func (s *seqSink) Emit(rows []record.Row) []record.Row {
	s.mu.Lock()
	for _, r := range rows {
		s.keys = append(s.keys, r.Key)
	}
	s.mu.Unlock()
	return rows[:0]
}

func TestOrderedWalkEqualsSortedUnordered(t *testing.T) {
	root := orderTree(t)
	engs := []string{EngineParallelLstat, EnginePortable}
	if runtime.GOOS == "linux" {
		engs = append(engs, EngineGetdents, EngineFstatat)
	}
	for _, noDirs := range []bool{false, true} {
		c := &collect{}
		if _, err := Walk(root, Options{Engine: EnginePortable, Workers: 3, Fields: FieldsStat, NoDirs: noDirs}, c); err != nil {
			t.Fatal(err)
		}
		var want []string
		for _, r := range c.rows {
			want = append(want, r.Key)
		}
		slices.Sort(want)
		if len(want) < 300 {
			t.Fatalf("tree too small: %d", len(want))
		}
		for _, eng := range engs {
			for _, tier := range []Fields{FieldsNames, FieldsStat} {
				type run struct {
					mode    string
					workers int
					bound   int
				}
				runs := []run{{OrderDFSSequential, 1, 0}, {OrderDFSParallel, 1, 1}, {OrderDFSParallel, 4, 3}, {OrderDFSParallel, 8, 2_000_000}}
				for _, rn := range runs {
					s := &seqSink{}
					st, err := WalkOrdered(root, rn.mode, Options{Engine: eng, Workers: rn.workers, Fields: tier, NoDirs: noDirs, LinkTarget: true}, rn.bound, s)
					if err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(s.keys, want) {
						t.Fatalf("%s %s %s w%d bound=%d noDirs=%v: sequence differs (%d vs %d keys): %v", eng, tier, rn.mode, rn.workers, rn.bound, noDirs, len(s.keys), len(want), firstDiff(s.keys, want))
					}
					for i := 1; i < len(s.keys); i++ {
						if s.keys[i-1] >= s.keys[i] {
							t.Fatalf("not strictly ascending at %d", i)
						}
					}
					if rn.mode == OrderDFSParallel && rn.bound > 0 && rn.bound < 1000 && st.ReorderMaxBuffered > int64(rn.bound)+400 {
						t.Errorf("reorder buffer %d exceeded bound %d by more than one directory", st.ReorderMaxBuffered, rn.bound)
					}
				}
			}
		}
	}
}

// The DFS rule itself, independent of the filesystem.
func TestDirectorySortRule(t *testing.T) {
	// Directory "a" (key "a/") and siblings: "a-b" < "a.b" < "a/" < "a/x" < "a0" < "ab".
	keys := []string{"a0", "a/", "ab", "a.b", "a-b"}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"a-b", "a.b", "a/", "a0", "ab"}) {
		t.Fatalf("%q", keys)
	}
}
