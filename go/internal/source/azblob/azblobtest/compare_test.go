// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/azure"
)

// TestCompareUTF16SortsTheCorpusAsTheReplay sorts the corpus with each
// comparator: the same order, which is not byte order.
func TestCompareUTF16SortsTheCorpusAsTheReplay(t *testing.T) {
	keys := Keys()
	mine, replay := slices.Clone(keys), slices.Clone(keys)
	slices.SortFunc(mine, CompareUTF16)
	slices.SortFunc(replay, func(a, b string) int { return azure.Compare([]byte(a), []byte(b)) })
	if !slices.Equal(mine, replay) {
		for i := range mine {
			if mine[i] != replay[i] {
				t.Fatalf("first difference at %d: %.40q against %.40q", i, mine[i], replay[i])
			}
		}
	}
	if slices.Equal(mine, keys) {
		t.Fatal("the corpus sorts the same in byte order")
	}
	if i := slices.Index(mine, "p\U00010000"); i < 0 || !strings.HasPrefix(mine[i+1], "p\U00010000") {
		t.Fatal("the F0..F4 branch is not contiguous")
	}
}
