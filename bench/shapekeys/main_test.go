// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/shapes"
)

// Every shape's keys under a prefix stay sorted, without repeats, and are
// the shape's keys with the prefix in front.
func TestPrefixedKeepsOrder(t *testing.T) {
	for _, s := range shapes.All {
		if s.Name == "maxlen" {
			continue
		}
		keys := s.Keys(2_000)
		got, err := prefixed(keys, "r2test/"+s.Name+"/", false)
		if err != nil {
			t.Fatalf("%s: %v", s.Name, err)
		}
		if !slices.IsSorted(got) || len(slices.Compact(slices.Clone(got))) != len(keys) {
			t.Fatalf("%s: the prefixed keys are not sorted and distinct", s.Name)
		}
		for i, k := range got {
			if k != "r2test/"+s.Name+"/"+keys[i] {
				t.Fatalf("%s: key %d is %q, not the shape's %q under the prefix", s.Name, i, k, keys[i])
			}
		}
	}
}

// maxlen's 1,024-byte keys do not fit under a prefix; with -overlay they
// keep their length and order.
func TestPrefixedMaxlen(t *testing.T) {
	s, _ := shapes.Lookup("maxlen")
	keys := s.Keys(2_000)
	if _, err := prefixed(keys, "r2test/maxlen/", false); err == nil || !strings.Contains(err.Error(), "more than 1024") {
		t.Fatalf("maxlen under a prefix: err %v, want the 1,024-byte refusal", err)
	}
	got, err := prefixed(keys, "r2test/maxlen/", true)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.IsSorted(got) || len(slices.Compact(slices.Clone(got))) != len(keys) {
		t.Fatal("the overlaid keys are not sorted and distinct")
	}
	for i, k := range got {
		if len(k) != 1024 || !strings.HasPrefix(k, "r2test/maxlen/") || k[14:] != keys[i][14:] {
			t.Fatalf("key %d is %q: want r2test/maxlen/ over the first 14 bytes of %q", i, k, keys[i])
		}
	}
}

// -overlay refuses keys that do not share the bytes the prefix replaces:
// replacing them could reorder the keys.
func TestPrefixedOverlayNeedsSharedHead(t *testing.T) {
	if _, err := prefixed([]string{"a/0001", "b/0002"}, "p/", true); err == nil {
		t.Fatal("overlay over differing heads: no error")
	}
	if _, err := prefixed([]string{"a"}, "p/", true); err == nil {
		t.Fatal("overlay longer than the key: no error")
	}
}

func TestWrite(t *testing.T) {
	var b bytes.Buffer
	if err := write(&b, []string{"a b", "c "}); err != nil || b.String() != "a b\nc \n" {
		t.Fatalf("write: %q, %v", b.String(), err)
	}
	if err := write(&b, []string{"a\rb"}); err == nil {
		t.Fatal("a key with a carriage return was written")
	}
}
