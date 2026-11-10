// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"testing"
)

// hostileKeys are names whose UTF-16 order differs from their byte order
// (U+E000 and U+FFFD against supplementary scalars), names Azure sends
// Encoded (U+FFFE, U+FFFF, with '+', '%' and spaces beside them), names
// that need escaping in a query or in XML, and enough under a/ and data/
// for many pages.
func hostileKeys() []string {
	keys := []string{"sp ace", "pl+us", "pct%41", "amp&<>", "q\"'", "a?b", "a#b",
		"u/\ue000", "u/\ufffd", "u/\U0001F600", "u/\U00010000", "u/\uffff", "u/\ufffe/x", "u/\ue000/y", "u/\U0001F600/z", "u/z",
		"e/\ufffe+plus", "e/\ufffe%2B", "e/\ufffe sp", "e/\uffff/\ufffe", "e/x\uffff+/y"}
	for i := range 23 {
		keys = append(keys, fmt.Sprintf("a/%03d", i), fmt.Sprintf("data/year=%d/part-%05d.parquet", 2020+i%3, i))
	}
	return keys
}

type lister interface {
	List(ctx context.Context, in *ListInput) (*ListOutput, error)
}

// walk lists in to its end, page by page.
func walk(t *testing.T, c lister, in ListInput) []ListOutput {
	t.Helper()
	var pages []ListOutput
	for {
		p, err := c.List(context.Background(), &in)
		if err != nil {
			t.Fatalf("%T %+v: %v", c, in, err)
		}
		pages = append(pages, *p)
		if p.NextMarker == "" || len(pages) > 20000 {
			return pages
		}
		in.Marker = p.NextMarker
	}
}
