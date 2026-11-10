// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/azure"
)

// validName is the replay's rule for a name it lists unencoded, which is
// Azure's: valid UTF-8, no control character, at most 1,024 UTF-16 code
// units, and no trailing dot (Azure trims it).
func validName(name string) error {
	switch {
	case name == "" || !utf8.ValidString(name):
		return fmt.Errorf("empty or not UTF-8")
	case strings.HasSuffix(name, "."):
		return fmt.Errorf("trailing dot")
	case len(utf16.Encode([]rune(name))) > 1024:
		return fmt.Errorf("over 1,024 UTF-16 code units")
	case strings.ContainsFunc(name, func(r rune) bool { return unicode.IsControl(r) || r == 0xfffe || r == 0xffff }):
		return fmt.Errorf("control character or noncharacter")
	}
	return nil
}

func TestKeysAreAValidHostileCorpus(t *testing.T) {
	keys := Keys()
	if !slices.IsSorted(keys) || len(slices.Compact(slices.Clone(keys))) != len(keys) {
		t.Fatal("not byte-sorted and unique")
	}
	if len(keys) <= 5000 || len(keys) > 6000 {
		t.Fatalf("%d names", len(keys))
	}
	if !slices.Equal(Keys(), keys) {
		t.Fatal("not deterministic")
	}
	lengths := map[int]int{}
	for _, k := range keys {
		if err := validName(k); err != nil {
			t.Errorf("%.40q: %v", k, err)
		}
		lengths[len(k)]++
	}
	if lengths[1024] != 1 || lengths[2010] != 1 || lengths[MaxNameBytes] != 2 {
		t.Errorf("long names: %d of 1,024 bytes, %d of 2,010, %d of 3,072", lengths[1024], lengths[2010], lengths[MaxNameBytes])
	}
	for _, want := range []string{"a/", "a/b", "dir/file.txt", "é", "", "ｚ", "�", "\U00010000", "\U0001f600",
		"only-s/\U0001f600", "only-e/ｚ", "r\U00010000\U00010000", "r\U00010000\U00010000\U00010000"} {
		if _, ok := slices.BinarySearch(keys, want); !ok {
			t.Errorf("lacks %q", want)
		}
	}
	// A sibling pair under the same parent at the root, one level down and
	// three deep: the two orders disagree on each.
	for _, parent := range []string{"", "d/", "n1/n2/n3/"} {
		e, s := parent+"p", parent+"p\U00010000"
		_, okE := slices.BinarySearch(keys, e)
		_, okS := slices.BinarySearch(keys, s)
		if !okE || !okS || e > s || azure.Compare([]byte(s), []byte(e)) >= 0 {
			t.Errorf("pair under %q", parent)
		}
	}
	// The only-child parents hold one name each.
	for _, p := range []string{"only-s/", "only-e/"} {
		n := 0
		for _, k := range keys {
			if strings.HasPrefix(k, p) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s holds %d names", p, n)
		}
	}
}

// TestServeLifecycle: Serve names its routes and Stop may be called more than once.
func TestServeLifecycle(t *testing.T) {
	r := Serve(t, []string{"a", "b/c"})
	if r.Account != "replay" || r.Container != Container || !strings.HasSuffix(r.Endpoint, "/replay") {
		t.Fatalf("%+v", r)
	}
	r.Stop()
	r.Stop()
}
