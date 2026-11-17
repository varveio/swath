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
	"time"

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
func level(t *testing.T, inv *Inventory, prefix string, limit int, totals *listing.SummaryOptions) (*listing.Entry, []listing.Entry, []*listing.Totals) {
	t.Helper()
	var self *listing.Entry
	var entries []listing.Entry
	var sums []*listing.Totals
	token := ""
	for n := 0; ; n++ {
		p, err := inv.Page(context.Background(), listing.PageRequest{Prefix: []byte(prefix), Token: token, Limit: limit, ChildTotals: totals})
		if err != nil {
			t.Fatalf("page %d of %q by %d: %v", n, prefix, limit, err)
		}
		if p.Order != listing.OrderBytes || p.Evidence.Stop != listing.StopCompleted || p.Evidence.Origin != listing.OriginInventory {
			t.Fatalf("page %d of %q: order %s, evidence %+v", n, prefix, p.Order, p.Evidence)
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
		if totals != nil && len(p.Totals) != len(p.Entries) {
			t.Fatalf("page %d of %q: %d totals for %d entries", n, prefix, len(p.Totals), len(p.Entries))
		}
		entries = append(entries, p.Entries...)
		sums = append(sums, p.Totals...)
		if p.Next == "" {
			return self, entries, sums
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
					self, got, _ := level(t, inv, prefix, limit, nil)
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
	if p, err := inv.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Token: first.Next, Limit: 1}); err != nil || string(p.Entries[0].Key) != "a/b0" {
		t.Fatalf("second page = %+v, %v", p, err)
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
	if _, err := decodeToken([]byte("a/"), inv.binding([]byte("a/"), nil), tokenVersion+"."+inv.binding([]byte("a/"), nil)+".spc1.r.Yg.0"); !errors.Is(err, listing.ErrInvalidToken) {
		t.Errorf("cursor outside the level: %v", err)
	}
}

// TestPageTokenOptions: a token continues only under the child totals it
// was issued with, the same kinds however spelt; a page's bounds of work
// may change.
func TestPageTokenOptions(t *testing.T) {
	ctx := context.Background()
	inv := open(t, write(t, fixture{id: reader.LocalV1, rows: tree()}))
	files := &listing.SummaryOptions{Kinds: []listing.Kind{listing.KindFile}}
	first, err := inv.Page(ctx, listing.PageRequest{Limit: 1, ChildTotals: files})
	if err != nil || first.Next == "" {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	for name, totals := range map[string]*listing.SummaryOptions{
		"no child totals":   nil,
		"every kind":        {},
		"directories":       {Kinds: []listing.Kind{listing.KindDir}},
		"files and objects": {Kinds: []listing.Kind{listing.KindFile, listing.KindObject}},
	} {
		if _, err := inv.Page(ctx, listing.PageRequest{Token: first.Next, Limit: 1, ChildTotals: totals}); !errors.Is(err, listing.ErrInvalidToken) {
			t.Errorf("%s: %v, want ErrInvalidToken", name, err)
		}
	}
	for name, req := range map[string]listing.PageRequest{
		"the same kinds spelt twice": {Limit: 1, ChildTotals: &listing.SummaryOptions{Kinds: []listing.Kind{listing.KindFile, listing.KindFile}}},
		"another limit and budget":   {Limit: 3, MaxDuration: time.Hour, ChildTotals: &listing.SummaryOptions{Kinds: files.Kinds, Budget: listing.Budget{MaxEntries: 1}}},
	} {
		req.Token = first.Next
		if _, err := inv.Page(ctx, req); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	all := []listing.Kind{listing.KindObject, listing.KindPrefix, listing.KindFile, listing.KindDir, listing.KindSymlink, listing.KindOther}
	every, err := inv.Page(ctx, listing.PageRequest{Limit: 1, ChildTotals: &listing.SummaryOptions{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inv.Page(ctx, listing.PageRequest{Token: every.Next, Limit: 1, ChildTotals: &listing.SummaryOptions{Kinds: all}}); err != nil {
		t.Errorf("every kind, listed: %v", err)
	}
}

// TestPageChildTotals: each child prefix or directory gets the totals of
// its subtree, counted as Summarize counts them, and no other entry any;
// totals the budget does not reach are nil, and the page stays complete.
func TestPageChildTotals(t *testing.T) {
	for _, c := range []struct {
		id    reader.SchemaID
		rows  []rec
		kinds []listing.Kind
	}{
		{reader.S3ObjectsV1, objects(), nil},
		{reader.AzureObjectsV1, objects(), []listing.Kind{listing.KindObject}},
		{reader.LocalV1, tree(), nil},
		{reader.LocalV1, tree(), []listing.Kind{listing.KindFile}},
		{reader.LocalV1, namesOnly(tree()), []listing.Kind{listing.KindFile}},
	} {
		isLocal := c.id == reader.LocalV1
		inv := open(t, write(t, fixture{id: c.id, rows: c.rows}))
		for _, prefix := range []string{"", "a/", "d/"} {
			if !isLocal && prefix == "d/" || isLocal && prefix == "a/" {
				continue
			}
			_, entries, totals := level(t, inv, prefix, 3, &listing.SummaryOptions{Kinds: c.kinds})
			for i, e := range entries {
				if e.Kind != listing.KindPrefix && e.Kind != listing.KindDir {
					if totals[i] != nil {
						t.Errorf("%s %q: %q has totals", c.id, prefix, e.Key)
					}
					continue
				}
				want := totalsOf(c.rows, isLocal, c.kinds, func(r rec) bool { return strings.HasPrefix(r.key, string(e.Key)) })
				if totals[i] == nil || !sameTotals(*totals[i], want) {
					t.Errorf("%s %v %q: totals of %q = %v, want %s", c.id, c.kinds, prefix, e.Key, totals[i], showTotals(want))
				}
			}
		}
	}

	ctx := context.Background()
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	for name, b := range map[string]listing.Budget{
		"value bytes": {MaxValueBytes: 1},
		"entries":     {MaxEntries: 3},
	} {
		p, err := inv.Page(ctx, listing.PageRequest{Limit: 100, ChildTotals: &listing.SummaryOptions{Budget: b}})
		if err != nil || p.Evidence.Stop != listing.StopCompleted || len(p.Entries) != 7 {
			t.Fatalf("%s: page %+v, %v", name, p, err)
		}
		for i, e := range p.Entries {
			small := string(e.Key) == "ab/" || string(e.Key) == "deep/" || string(e.Key) == "nulls/"
			if p.Totals[i] != nil && (name == "value bytes" || !small) {
				t.Errorf("%s: %q has totals %s past the budget", name, e.Key, showTotals(*p.Totals[i]))
			}
		}
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
// kind is ErrNotDirectory; one it lacks is ErrNotFound when it proves the
// absence, an empty page otherwise; an empty directory is its Self alone.
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
	if p, err := page(partial, "missing/"); err != nil || len(p.Entries) != 0 || p.Self != nil {
		t.Errorf("missing directory, absence unproven: %+v, %v", p, err)
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

// TestPageTimeBudget: a page its MaxDuration ends holds no entry, says so,
// and its Next starts it again.
func TestPageTimeBudget(t *testing.T) {
	dir := write(t, fixture{id: reader.S3ObjectsV1, rows: objects()})
	inv := openWith(t, dir, newSlowSource(t, dir, 0))
	first, err := inv.Page(context.Background(), listing.PageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	slow := openWith(t, dir, newSlowSource(t, dir, 50*time.Millisecond))
	for _, token := range []string{"", first.Next} {
		p, err := slow.Page(context.Background(), listing.PageRequest{Token: token, Limit: 2, MaxDuration: 5 * time.Millisecond})
		if err != nil || p.Evidence.Stop != listing.StopTimeBudget || len(p.Entries) != 0 || p.Next != token {
			t.Errorf("page after %q = %+v, %v", token, p, err)
		}
	}
}
