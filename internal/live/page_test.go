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

// byteKeys is an answering store whose keys are bytes (ByteKeys).
type byteKeys struct{ answering }

func (byteKeys) ByteKeys() {}

// TestPageDirectoriesAndByteKeys: a level may hold directories as KindDir
// entries, one name and a "/" each, and a store of byte keys may list and
// be asked for a key that is not UTF-8; a store of UTF-8 keys may not.
func TestPageDirectoriesAndByteKeys(t *testing.T) {
	ctx := context.Background()
	size := int64(4096)
	entry := func(key string, kind listing.Kind) listing.Entry {
		return listing.Entry{Key: []byte(key), Kind: kind, Size: &size}
	}
	lvl := Level{Objects: []listing.Entry{entry("a/", listing.KindDir), entry("a/d/", listing.KindDir), entry("a/\xff", listing.KindFile)}}
	req := listing.PageRequest{Prefix: []byte("a/"), Limit: 10}

	page, err := NewSource(byteKeys{answering{lvl}}).Page(ctx, req)
	if err != nil || page.Self == nil || page.Self.Kind != listing.KindDir || len(page.Entries) != 2 ||
		page.Entries[0].Kind != listing.KindDir || page.Entries[0].Size == nil || string(page.Entries[1].Key) != "a/\xff" {
		t.Fatalf("byte keys: %+v, %v", page, err)
	}
	if _, err := NewSource(byteKeys{answering{lvl}}).Lookup(ctx, []byte("a/\xff")); err != nil {
		t.Errorf("byte keys, a lookup: %v", err)
	}

	if _, err := NewSource(answering{lvl}).Page(ctx, req); !errors.Is(err, listing.ErrInvalidPage) {
		t.Errorf("UTF-8 keys, a page: %v, want ErrInvalidPage", err)
	}
	if _, err := NewSource(answering{lvl}).Lookup(ctx, []byte("a/\xff")); !errors.Is(err, listing.ErrInvalidRequest) {
		t.Errorf("UTF-8 keys, a lookup: %v, want ErrInvalidRequest", err)
	}
	deep := Level{Objects: []listing.Entry{entry("a/d/e/", listing.KindDir)}}
	if _, err := NewSource(byteKeys{answering{deep}}).Page(ctx, req); !errors.Is(err, listing.ErrInvalidPage) {
		t.Errorf("a directory below a child: %v, want ErrInvalidPage", err)
	}
}
