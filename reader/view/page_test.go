// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// scopes are the root, every prefix of every key at a "/", and on an
// object store prefixes that end elsewhere or hold nothing (a local
// directory that does not exist is ErrNotFound).
func scopes(rs []rec, isLocal bool) []string {
	out := []string{""}
	if !isLocal {
		out = append(out, "zz-empty/", "a/nothing/", "a", "bu", "a/b")
	}
	for _, r := range rs {
		for i := range len(r.key) {
			if r.key[i] == '/' {
				out = append(out, r.key[:i+1])
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// level pages through prefix's level, checking each page's shape, and
// returns its Self and entries.
func level(t *testing.T, inv *Inventory, prefix string, limit int) (*listing.Entry, []listing.Entry) {
	t.Helper()
	var self *listing.Entry
	var entries []listing.Entry
	token := ""
	for n := 0; ; n++ {
		p, err := inv.Page(context.Background(), listing.PageRequest{Prefix: []byte(prefix), Token: token, Limit: limit})
		if err != nil {
			t.Fatalf("page %d of %q by %d: %v", n, prefix, limit, err)
		}
		if len(p.Entries) > limit || p.Next != "" && len(p.Entries) != limit {
			t.Fatalf("page %d of %q by %d holds %d entries, next %q", n, prefix, limit, len(p.Entries), p.Next)
		}
		if p.Self != nil && n > 0 {
			t.Fatalf("page %d of %q has Self", n, prefix)
		}
		if n == 0 {
			self = p.Self
		}
		entries = append(entries, p.Entries...)
		if p.Next == "" {
			return self, entries
		}
		token = p.Next
	}
}

// TestPageMatchesTheRecordsPageByPage pages every level of every schema's
// fixture in pages of several sizes, across key pages and parts, and
// checks each against the level the records alone give: the children in
// byte order, the entry at exactly the prefix as Self, every value as
// written.
func TestPageMatchesTheRecordsPageByPage(t *testing.T) {
	for _, id := range append(slices.Clone(objectSchemas), reader.LocalV1) {
		t.Run(string(id), func(t *testing.T) {
			isLocal := id == reader.LocalV1
			rs := objects()
			if isLocal {
				rs = tree()
			}
			inv := open(t, write(t, fixture{id: id, rows: rs}))
			byKey := map[string]rec{}
			for _, r := range rs {
				byKey[r.key] = r
			}
			for _, prefix := range scopes(rs, isLocal) {
				wantSelf, want := children(rs, prefix, isLocal)
				for _, limit := range []int{1, 2, 7, 1000} {
					self, got := level(t, inv, prefix, limit)
					if (self == nil) != (wantSelf == nil) || self != nil && !sameEntry(*self, *wantSelf, isLocal) {
						t.Fatalf("Self of %q by %d = %+v, want %+v", prefix, limit, self, wantSelf)
					}
					if len(got) != len(want) {
						t.Fatalf("%q by %d: %d entries, want %d (%v)", prefix, limit, len(got), len(want), want)
					}
					for i, e := range got {
						if string(e.Key) != want[i].key || e.Kind != want[i].kind {
							t.Fatalf("%q by %d entry %d = %q %s, want %q %s", prefix, limit, i, e.Key, e.Kind, want[i].key, want[i].kind)
						}
						if r, ok := byKey[want[i].key]; ok && e.Kind != listing.KindPrefix && !sameEntry(e, r, isLocal) {
							t.Fatalf("%q entry %+v, want %+v", prefix, e, r)
						}
					}
				}
			}
		})
	}
}

// TestPageTokens: a token continues only the level it was issued for, of
// the inventory that issued it.
func TestPageTokens(t *testing.T) {
	ctx := context.Background()
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	other := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()[:5]}))
	first, err := inv.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1})
	if err != nil || first.Next == "" || !strings.HasPrefix(first.Next, tokenVersion+".") {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	if p, err := inv.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Token: first.Next, Limit: 3}); err != nil || string(p.Entries[0].Key) != "a/b0" {
		t.Fatalf("second page, another limit = %+v, %v", p, err)
	}
	for name, c := range map[string]struct {
		inv    *Inventory
		prefix string
		token  string
	}{
		"another prefix":    {inv, "a", first.Next},
		"another inventory": {other, "a/", first.Next},
		"another version":   {inv, "a/", "inv2" + strings.TrimPrefix(first.Next, tokenVersion)},
		"garbage":           {inv, "a/", "inv1.x.y"},
		"no cursor":         {inv, "a/", first.Next[:strings.LastIndexByte(first.Next, '.')]},
	} {
		if _, err := c.inv.Page(ctx, listing.PageRequest{Prefix: []byte(c.prefix), Token: c.token, Limit: 1}); !errors.Is(err, listing.ErrInvalidToken) {
			t.Errorf("%s: %v, want ErrInvalidToken", name, err)
		}
	}
	// A cursor outside the level, bound as a real token is: refused.
	if _, err := decodeToken([]byte("a/"), inv.binding([]byte("a/")), tokenVersion+"."+inv.binding([]byte("a/"))+".spc1.r.Yg.0"); !errors.Is(err, listing.ErrInvalidToken) {
		t.Errorf("cursor outside the level: %v", err)
	}
}

// TestPageScope: an inventory of one prefix pages only inside it.
func TestPageScope(t *testing.T) {
	ctx := context.Background()
	ext := extension(reader.S3ObjectsV1)
	ext["scope"] = map[string]any{"kind": "prefix", "prefix": "a/"}
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, ext: ext, rows: objects()[1:5]}))
	for _, prefix := range []string{"", "a", "b/"} {
		if _, err := inv.Page(ctx, listing.PageRequest{Prefix: []byte(prefix), Limit: 5}); !errors.Is(err, listing.ErrOutOfScope) {
			t.Errorf("page of %q = %v, want ErrOutOfScope", prefix, err)
		}
	}
	p, err := inv.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 5})
	if err != nil || p.Self == nil || len(p.Entries) != 3 || !bytes.Equal(p.Evidence.Scope, []byte("a/")) {
		t.Fatalf("page of the scope = %+v, %v", p, err)
	}
}

// TestPageMissingDirectory: a local directory the inventory has as another
// kind, or below a symbolic link, is ErrNotDirectory; one it lacks is
// ErrNotFound when it proves the
// absence, an empty page otherwise, and always at or under an entry of
// unknown kind; an empty directory is its Self alone.
func TestPageMissingDirectory(t *testing.T) {
	ctx := context.Background()
	complete := open(t, write(t, fixture{id: reader.LocalV1, rows: tree()}))
	partial := open(t, write(t, fixture{id: reader.LocalV1, rows: tree(),
		ext: coverage(extension(reader.LocalV1), map[string]any{"complete": false, "unreadable": 1})}))
	page := func(inv *Inventory, prefix string) (listing.Page, error) {
		return inv.Page(ctx, listing.PageRequest{Prefix: []byte(prefix), Limit: 10})
	}
	if _, err := page(complete, "missing/"); !errors.Is(err, listing.ErrNotFound) {
		t.Errorf("missing directory: %v, want ErrNotFound", err)
	}
	for _, inv := range []*Inventory{complete, partial} {
		if _, err := page(inv, "a.txt/"); !errors.Is(err, listing.ErrNotDirectory) {
			t.Errorf("a file: %v, want ErrNotDirectory", err)
		}
	}
	// A symbolic link is never followed: a directory below one is not
	// reached, never absent.
	for _, prefix := range []string{"link/", "link/x/"} {
		if _, err := page(complete, prefix); !errors.Is(err, listing.ErrNotDirectory) {
			t.Errorf("%q through a symbolic link: %v, want ErrNotDirectory", prefix, err)
		}
	}
	if p, err := page(partial, "missing/"); err != nil || len(p.Entries) != 0 || p.Self != nil {
		t.Errorf("missing directory, absence unproven: %+v, %v", p, err)
	}
	// An entry of unknown kind may be a directory the listing did not
	// read: neither it nor anything under it is another kind or absent.
	for _, prefix := range []string{"u/", "u/x/"} {
		if p, err := page(complete, prefix); err != nil || len(p.Entries) != 0 || p.Self != nil {
			t.Errorf("%q under an entry of unknown kind: %+v, %v", prefix, p, err)
		}
	}
	if p, err := page(complete, "empty/"); err != nil || len(p.Entries) != 0 || p.Self == nil || p.Self.Kind != listing.KindDir {
		t.Errorf("empty directory: %+v, %v", p, err)
	}
	// An object store has no directories: a prefix with nothing under it
	// is an empty page.
	objects := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	if p, err := page(objects, "missing/"); err != nil || len(p.Entries) != 0 {
		t.Errorf("object store, empty prefix: %+v, %v", p, err)
	}
}
