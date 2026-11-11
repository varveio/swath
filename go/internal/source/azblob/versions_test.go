// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// versionEntries gives each name 1 to 5 entries in Azure's order within a
// name, by its index: snapshots, previous versions (some soft-deleted),
// then the current blob, live, soft-deleted or absent (a versioned blob
// deleted: only previous versions remain). Each entry's Size identifies it.
func versionEntries(names []string) []client.Item {
	var out []client.Item
	for i, name := range names {
		n := 1 + i%5
		for j := range n {
			it := client.Item{Name: name, Size: int64(len(out)), BlobType: "BlockBlob"}
			last := j == n-1
			switch {
			case !last && j%2 == 0:
				it.Snapshot = fmt.Sprintf("2026-01-01T00:00:%02d.0000000Z", j)
			case !last:
				it.VersionID = fmt.Sprintf("2026-01-02T00:00:%02d.0000000Z", j)
				it.Deleted = i%3 == 0
			case i%7 == 3:
				it.Deleted, it.DeletedTime, it.RetentionDays, it.HasRetention = true, 1, int64(i%4), true
			case i%7 == 5:
				it.VersionID = "2026-01-03T00:00:00.0000000Z" // a previous version: no current one
			case i%2 == 0:
				it.VersionID, it.IsCurrentVersion = "2026-01-04T00:00:00.0000000Z", true
			}
			out = append(out, it)
		}
	}
	return out
}

// wantVersions is every entry, names in byte order, a name's entries in
// the order given.
func wantVersions(entries []client.Item, after string) []string {
	var out []string
	sorted := slices.Clone(entries)
	slices.SortStableFunc(sorted, func(a, b client.Item) int { return strings.Compare(a.Name, b.Name) })
	for _, it := range sorted {
		if it.Name > after {
			out = append(out, fmt.Sprintf("%q#%d", it.Name, it.Size))
		}
	}
	return out
}

type vcollect struct {
	mu   sync.Mutex
	rows []VersionRow
}

func (c *vcollect) Write(_ context.Context, segs [][]VersionRow) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		c.rows = append(c.rows, s...)
	}
	return nil
}

func versionIDs(rows []VersionRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%q#%d", r.Key, r.Data.Size)
	}
	return out
}

// versionsEngine is a versions provider over st whose engine has the
// options given.
func versionsEngine(t *testing.T, st *azblobtest.Store, opt keyrange.Options) *VersionsProvider {
	t.Helper()
	p, err := NewVersions(context.Background(), Options{Account: "acct", Container: "c", Client: st, Workers: max(1, opt.Workers)})
	if err != nil {
		t.Fatal(err)
	}
	e, err := keyrange.New(opt, p.description(), p.fetch)
	if err != nil {
		t.Fatal(err)
	}
	p.Engine = e
	return p
}

func runVersions(t *testing.T, p *VersionsProvider, workers int, after *string) ([]VersionRow, error) {
	t.Helper()
	c := &vcollect{}
	_, err := order.Run[Range, Version](context.Background(), p, c, &VersionSchema,
		order.Options{Workers: workers, Budget: 1 << 22, Groups: true, After: after})
	return c.rows, err
}

// versionNames are names at U+E000 and above beside names above U+FFFF
// (the adapter splits spans there), under folders, and a filler run long
// enough for splits at every page size.
func versionNames() []string {
	names := utf16Keys()
	for i := range 300 {
		names = append(names, fmt.Sprintf("f/%03d", i))
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// TestVersionsListing: a container whose names have up to five entries
// each lists every entry once, a name's entries consecutive and in
// Azure's order, sequentially and in parallel, with the seed and
// structure, at page sizes that put page seams inside names (1, 2) and
// with short responses; every request asks for versions, snapshots and
// soft-deleted blobs, a probe's too; ranges split (completed names only).
func TestVersionsListing(t *testing.T) {
	entries := versionEntries(versionNames())
	want := wantVersions(entries, "")
	all := client.Include{Versions: true, Snapshots: true, Deleted: true}
	split, held, open := false, false, false
	for _, page := range []int{1, 2, 7, 1000} {
		for _, short := range []bool{false, true} {
			for i, opt := range []keyrange.Options{
				{Sequential: true},
				{Workers: 8},
				{Workers: 8, Seed: true, Structure: keyrange.StructureOn},
			} {
				what := fmt.Sprintf("page=%d short=%v opt %d", page, short, i)
				st := azblobtest.NewEntryStore(entries)
				if short {
					st.Short = func(n int) int { return 1 + n%3 }
				}
				opt.PageSize = page
				p := versionsEngine(t, st, opt)
				rows, err := runVersions(t, p, max(1, opt.Workers), nil)
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if got := versionIDs(rows); !slices.Equal(got, want) {
					t.Fatalf("%s: %d entries, want %d", what, len(got), len(want))
				}
				for _, r := range st.Log() {
					inc := all
					if r.In.Delimiter != "" {
						inc.Snapshots = false
					}
					if r.In.Include != inc {
						t.Fatalf("%s: sent %+v", what, r.In.Include)
					}
				}
				v := p.Stats().Counters.Value
				split = split || v("split.accepted_bounded")+v("split.accepted_frontier") > 0
				held = held || v("groups.held_rows") > 0
				open = open || v("groups.open_pages") > 0
			}
		}
	}
	if !split || !held || !open {
		t.Errorf("engaged: splits %v, held rows %v, open groups %v", split, held, open)
	}
}

// TestVersionsRestart: a resume after any name lists every entry of the
// names after it and none of its own, however many pages they fill (the
// adapter's exclusive start drops each of them).
func TestVersionsRestart(t *testing.T) {
	names := versionNames()
	entries := versionEntries(names)
	for i, after := range names {
		if i%11 != 0 && i != len(names)-1 {
			continue
		}
		for _, page := range []int{1, 1000} {
			st := azblobtest.NewEntryStore(entries)
			p := versionsEngine(t, st, keyrange.Options{Workers: 4, PageSize: page})
			if err := p.Restart(after); err != nil {
				t.Fatal(err)
			}
			rows, err := runVersions(t, p, 4, &after)
			if err != nil {
				t.Fatalf("after %q page %d: %v", after, page, err)
			}
			if got, want := versionIDs(rows), wantVersions(entries, after); !slices.Equal(got, want) {
				t.Fatalf("after %q page %d: %d entries, want %d", after, page, len(got), len(want))
			}
		}
	}
}

// TestVersionRoles: is_latest is the entry a current listing lists (the
// base blob, live, current where versioned); the version id and snapshot
// are as listed, null when absent; a soft-deleted entry keeps its size;
// the schema's version roles bind, with no delete markers.
func TestVersionRoles(t *testing.T) {
	for _, c := range []struct {
		e    client.Entry
		want bool
	}{
		{client.Entry{}, true},
		{client.Entry{VersionID: "v", IsCurrentVersion: true}, true},
		{client.Entry{VersionID: "v"}, false},
		{client.Entry{Snapshot: "s"}, false},
		{client.Entry{Snapshot: "s", VersionID: "v", IsCurrentVersion: true}, false},
		{client.Entry{Deleted: true}, false},
		{client.Entry{Deleted: true, VersionID: "v", IsCurrentVersion: true}, false},
	} {
		if got := latest(&c.e); got != c.want {
			t.Errorf("%+v: latest %v", c.e, got)
		}
	}
	v, err := schema.BindVersions(&VersionSchema)
	if err != nil || v == nil || v.HasDeleteMarkers() {
		t.Fatalf("roles %+v, %v", v, err)
	}
	row := Version{Object: Object{Size: 9}, Snapshot: "s", Deleted: true, DeletedTime: 5, HasRetention: true}
	if id, ok := v.ID(&row); ok || id != "" || v.Latest(&row) {
		t.Fatalf("snapshot row: id (%q, %v), latest %v", id, ok, v.Latest(&row))
	}
	if names := VersionSchema.Specs(); len(names) != 1+24+6 || names[25].Name != "version_id" || names[30].Name != "remaining_retention_days" {
		t.Fatalf("specs %+v", names)
	}
	size := VersionSchema.Columns[0]
	if n, ok := size.Int(&row); !ok || n != 9 {
		t.Fatalf("a soft-deleted entry's size: (%d, %v)", n, ok)
	}
	for i, c := range Schema.Columns {
		if VersionSchema.Columns[i].Spec != c.Spec {
			t.Fatalf("column %d: %+v, the current schema's %+v", i, VersionSchema.Columns[i].Spec, c.Spec)
		}
	}
}

// TestVersionCodec: every field round-trips through the plain and the
// delta encoding, and a flags byte with an unknown bit is refused.
func TestVersionCodec(t *testing.T) {
	rows := []Version{
		{Object: Object{Size: 1, ETag: "e", Props: client.Props{LeaseState: "available", ServerEncrypted: client.True}},
			VersionID: "2026-01-02T00:00:00.0000000Z", IsLatest: true},
		{Object: Object{Size: 2}, Snapshot: "2026-01-01T00:00:00.0000000Z", Deleted: true, DeletedTime: 7, RetentionDays: 0, HasRetention: true},
		{Object: Object{Size: 3}, VersionID: "2026-01-02T00:00:00.0000000Z", RetentionDays: 3, HasRetention: true},
		{},
	}
	c := versionCodec{}
	var prev Version
	for i, row := range rows {
		var got, gotDelta Version
		d := schema.NewOwningDecoder(c.Append(nil, &row), 1024)
		if c.Decode(d, &got); d.Err() != nil || d.Len() != 0 || got != row {
			t.Fatalf("row %d: %+v, %v", i, got, d.Err())
		}
		d = schema.NewOwningDecoder(c.AppendDelta(nil, &prev, &row), 1024)
		if c.DecodeDelta(d, &prev, &gotDelta); d.Err() != nil || d.Len() != 0 || gotDelta != row {
			t.Fatalf("row %d delta: %+v, %v", i, gotDelta, d.Err())
		}
		prev = row
	}
	enc := c.Append(nil, &Version{})
	at := len(objectCodec{}.Append(nil, &Object{}))
	enc[at] = 0x08
	var v Version
	d := schema.NewOwningDecoder(enc, 1024)
	if c.Decode(d, &v); d.Err() == nil {
		t.Fatal("an unknown flag accepted")
	}
}

// TestAllVersionsOpen: --all-versions opens the versions listing, its rows
// bound to swath/azblob-versions/v1, resumable as the current listing.
func TestAllVersionsOpen(t *testing.T) {
	src, err := open(context.Background(), registry.Request{Root: "az://acct/c",
		Options: &Options{Client: &routed{pages: &pages{}}, AllVersions: true}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := src.(*opened[Version]); !ok {
		t.Fatalf("%T", src)
	}
	if src.(registry.Source).Schema().ID != "swath/azblob-versions/v1" || !src.(source.Capable).Capabilities().Resume {
		t.Fatalf("schema %s, capabilities %+v", src.(registry.Source).Schema().ID, src.(source.Capable).Capabilities())
	}
}
