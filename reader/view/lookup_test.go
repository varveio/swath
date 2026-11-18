// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"
	"testing"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// TestLookup: present where the inventory has a row; absent only inside
// its scope from a complete, unfiltered listing; unknown otherwise, and
// unknown with ErrOutOfScope outside its scope.
func TestLookup(t *testing.T) {
	ctx := context.Background()
	rs := objects()
	prefixed := extension(reader.S3ObjectsV1)
	prefixed["scope"] = map[string]any{"kind": "prefix", "prefix": "a/"}
	listed := func(id reader.SchemaID, set map[string]any) *Inventory {
		return open(t, write(t, fixture{id: id, rows: rs, ext: coverage(extension(id), set)}))
	}
	inventories := map[string]*Inventory{
		"complete":   listed(reader.GCSObjectsV1, nil),
		"unreadable": listed(reader.S3ObjectsV1, map[string]any{"complete": false, "unreadable": 2}),
		"stopped":    listed(reader.S3ObjectsV1, map[string]any{"complete": false, "stop_reason": "max_keys"}),
		"filtered":   listed(reader.AzureObjectsV1, map[string]any{"filters": []any{map[string]any{"filter": "min_size", "value": "1"}}}),
		"excluded":   listed(reader.S3ObjectsV1, map[string]any{"exclusions": []string{"out/"}}),
		"prefixed":   open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: rs[1:5], ext: prefixed})),
	}
	for _, c := range []struct {
		inv, key string
		state    listing.LookupState
		reason   error
	}{
		{"complete", "a/", listing.LookupPresent, nil},
		{"complete", "bulk/00299", listing.LookupPresent, nil},
		{"complete", "a/c/", listing.LookupAbsent, nil}, // a prefix, no object
		{"complete", "missing", listing.LookupAbsent, nil},
		{"unreadable", "z", listing.LookupPresent, nil},
		{"unreadable", "missing", listing.LookupUnknown, nil},
		{"stopped", "missing", listing.LookupUnknown, nil},
		{"filtered", "missing", listing.LookupUnknown, nil},
		{"excluded", "missing", listing.LookupUnknown, nil},
		{"prefixed", "a/b", listing.LookupPresent, nil},
		{"prefixed", "a/missing", listing.LookupAbsent, nil},
		{"prefixed", "a", listing.LookupUnknown, listing.ErrOutOfScope},
		{"prefixed", "z", listing.LookupUnknown, listing.ErrOutOfScope},
	} {
		r, err := inventories[c.inv].Lookup(ctx, []byte(c.key))
		if err != nil || r.State != c.state || (c.reason == nil) != (r.Reason == nil) || c.reason != nil && !errors.Is(r.Reason, c.reason) {
			t.Errorf("%s: Lookup(%q) = %v %v, %v; want %v %v", c.inv, c.key, r.State, r.Reason, err, c.state, c.reason)
			continue
		}
		if (r.State == listing.LookupPresent) != (r.Entry != nil) || r.Entry != nil && string(r.Entry.Key) != c.key {
			t.Errorf("%s: Lookup(%q) entry = %+v", c.inv, c.key, r.Entry)
		}
		if r.Evidence.Stop != listing.StopCompleted {
			t.Errorf("%s: Lookup(%q) evidence = %+v", c.inv, c.key, r.Evidence)
		}
	}

	byKey := map[string]rec{}
	for _, r := range rs {
		byKey[r.key] = r
	}
	r, _ := inventories["complete"].Lookup(ctx, []byte("nulls/no-metadata"))
	if r.Entry == nil || !sameEntry(*r.Entry, byKey["nulls/no-metadata"], false) {
		t.Errorf("entry = %+v", r.Entry)
	}
}

// TestLookupLocal: a local key names a file or, without a trailing "/", a
// directory too, answered with its directory key; a directory key names
// a directory only. The root has no entry. Nothing at or under an entry
// of unknown kind is absent; a key below a symbolic link is not reached,
// on a listing that may lack entries too.
func TestLookupLocal(t *testing.T) {
	ctx := context.Background()
	inv := open(t, write(t, fixture{id: reader.LocalV1, rows: tree()}))
	for _, c := range []struct {
		key, entry string
		state      listing.LookupState
		kind       listing.Kind
	}{
		{"a.txt", "a.txt", listing.LookupPresent, listing.KindFile},
		{"d", "d/", listing.LookupPresent, listing.KindDir},
		{"d/", "d/", listing.LookupPresent, listing.KindDir},
		{"d/nested", "d/nested/", listing.LookupPresent, listing.KindDir},
		{"link", "link", listing.LookupPresent, listing.KindSymlink},
		{"fifo", "fifo", listing.LookupPresent, listing.KindOther},
		{"u", "u", listing.LookupPresent, listing.KindUnknown},
		{"bad\xffname.txt", "bad\xffname.txt", listing.LookupPresent, listing.KindFile},
		{"a.txt/", "", listing.LookupAbsent, 0},
		{"d/missing", "", listing.LookupAbsent, 0},
	} {
		r, err := inv.Lookup(ctx, []byte(c.key))
		if err != nil || r.State != c.state || c.entry != "" && (r.Entry == nil || string(r.Entry.Key) != c.entry || r.Entry.Kind != c.kind) {
			t.Errorf("Lookup(%q) = %v %+v, %v; want %v %q %v", c.key, r.State, r.Entry, err, c.state, c.entry, c.kind)
		}
	}
	// An entry of unknown kind may be a directory the listing did not
	// read: nothing at or under it is absent. A symbolic link is never
	// followed: nothing below it is reached. Under another kind a key is
	// absent.
	for key, want := range map[string]error{
		"u/": errUnresolvedKind, "u/x": errUnresolvedKind, "u/x/y/": errUnresolvedKind,
		"link/": listing.ErrNotDirectory, "link/x": listing.ErrNotDirectory, "link/x/y": listing.ErrNotDirectory,
		"fifo/x": nil, "a.txt/x": nil,
	} {
		r, err := inv.Lookup(ctx, []byte(key))
		if err != nil || (want == nil) != (r.State == listing.LookupAbsent) || want != nil && (r.State != listing.LookupUnknown || !errors.Is(r.Reason, want)) {
			t.Errorf("Lookup(%q) = %v %v, %v; want the reason %v", key, r.State, r.Reason, err, want)
		}
	}
	if _, err := inv.Lookup(ctx, nil); !errors.Is(err, listing.ErrInvalidRequest) {
		t.Errorf("Lookup of the root: %v", err)
	}

	// A listing that may lack entries still names the link on the way.
	partial := open(t, write(t, fixture{id: reader.LocalV1, rows: tree(),
		ext: coverage(extension(reader.LocalV1), map[string]any{"complete": false, "unreadable": 1})}))
	if r, err := partial.Lookup(ctx, []byte("link/x")); err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrNotDirectory) {
		t.Errorf("partial, Lookup(link/x) = %v %v, %v; want ErrNotDirectory", r.State, r.Reason, err)
	}
}

// TestLookupCancelled: a cancelled ctx is the only failure Lookup returns.
func TestLookupCancelled(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := inv.Lookup(ctx, []byte("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Lookup = %v, want the context's error", err)
	}
}
