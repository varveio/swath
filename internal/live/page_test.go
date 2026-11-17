// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"errors"
	"testing"

	"github.com/varveio/swath/reader/listing"
)

// answering is a store whose every listing is lvl.
type answering struct{ lvl Level }

func (answering) Location() string { return "answering" }

func (a answering) List(context.Context, []byte, string, int) (*Level, error) {
	return &a.lvl, nil
}

func (answering) Lookup(context.Context, []byte) (listing.LookupResult, error) {
	return listing.LookupResult{}, nil
}

func (answering) Close() error { return nil }

// TestPageLimitCountsChildren: Limit bounds a page's children, and the
// entry at exactly the prefix comes beside them; more children than asked
// for is ErrInvalidPage.
func TestPageLimitCountsChildren(t *testing.T) {
	size := int64(1)
	object := func(key string) listing.Entry {
		return listing.Entry{Key: []byte(key), Kind: listing.KindObject, Size: &size}
	}
	req := listing.PageRequest{Prefix: []byte("a/"), Limit: 2}

	src := NewSource(answering{Level{Objects: []listing.Entry{object("a/"), object("a/x"), object("a/y")}}})
	page, err := src.Page(context.Background(), req)
	if err != nil || page.Self == nil || len(page.Entries) != 2 {
		t.Fatalf("Self and two children: %+v, %v", page, err)
	}

	src = NewSource(answering{Level{Objects: []listing.Entry{object("a/w"), object("a/x"), object("a/y")}}})
	if _, err := src.Page(context.Background(), req); !errors.Is(err, listing.ErrInvalidPage) {
		t.Fatalf("three children: %v, want ErrInvalidPage", err)
	}
}
