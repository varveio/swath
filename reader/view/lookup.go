// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"context"
	"errors"
	"slices"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// Lookup says what is at exactly key: present when the inventory has a row
// for it; absent only inside the inventory's scope and when its Unlocated
// is 0, as of the listing; otherwise unknown. A key outside the scope is
// unknown with ErrOutOfScope as its Reason, a key at or under a local
// entry of unknown kind is unknown with a Reason that says so, a key
// below a symbolic link is unknown with a Reason of ErrNotDirectory (not
// reached: no listing follows a link), and a read that fails leaves it
// unknown with the failure as its Reason. On a local
// inventory a key without a trailing "/" is also looked up as a
// directory's key. An empty key, or one holding a NUL, is
// ErrInvalidRequest. It returns an error only for such a request, and when
// ctx ends.
func (inv *Inventory) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	if err := inv.checkKey(key); err != nil {
		return listing.LookupResult{}, err
	}
	result := listing.LookupResult{Evidence: inv.evidence(listing.StopCompleted)}
	if !inv.inScope(key) {
		result.Reason = errOutOfScope
		return result, nil
	}

	found, err := inv.entries(ctx, inv.keys(key))
	if err != nil {
		if ctx.Err() != nil {
			return listing.LookupResult{}, ctx.Err()
		}
		result.Reason = failed(ctx, err)
		return result, nil
	}
	result.State, result.Entry, result.Reason = inv.resolve(key, found)
	return result, nil
}

// keys are the keys whose entries answer what is at key: its names and,
// when the inventory proves absence, its ancestors.
func (inv *Inventory) keys(key []byte) [][]byte {
	keys := inv.names(key)
	if inv.provable {
		keys = append(keys, inv.ancestors(key)...)
	}
	return keys
}

// names are the keys an entry at key may have: key, and on a local
// inventory the directory key "key/" of a key without a trailing "/".
func (inv *Inventory) names(key []byte) [][]byte {
	if !inv.rows.local || key[len(key)-1] == '/' {
		return [][]byte{key}
	}
	return [][]byte{key, append(bytes.Clone(key), '/')}
}

// ancestors are the names on the way to a local key, each component but
// the last without its trailing "/": "a" and "a/b" for "a/b/c" and for
// "a/b/". An entry of unknown kind at one of them may be a directory the
// listing did not read, so it proves nothing absent at or under it. An
// object store has none.
func (inv *Inventory) ancestors(key []byte) [][]byte {
	if !inv.rows.local {
		return nil
	}
	var out [][]byte
	for i, c := range key {
		if c == '/' && i > 0 {
			out = append(out, key[:i])
		}
	}
	return out
}

var (
	// errUnresolvedKind is the Reason of a key the inventory cannot prove
	// absent because an entry on the way to it is of unknown kind.
	errUnresolvedKind = errors.New("view: an entry on the way to the key is of a kind the listing did not resolve")
	// errThroughLink is the Reason of a key below a symbolic link, which
	// no listing follows: not reached, never absent. Its class is the one
	// a live local source gives a path through a link.
	errThroughLink = wrap(listing.ErrNotDirectory, "a symbolic link on the way to the key is not followed")
)

// resolve answers what is at key from the entries found at its keys: the
// entry at key itself before a directory's; else absent when the
// inventory proves it and no ancestor found is of unknown kind or a
// symbolic link; else unknown, with errUnresolvedKind or errThroughLink
// when such an ancestor is the reason.
func (inv *Inventory) resolve(key []byte, found map[string]*listing.Entry) (listing.LookupState, *listing.Entry, error) {
	for _, name := range inv.names(key) {
		if e := found[string(name)]; e != nil {
			return listing.LookupPresent, cloneEntry(e), nil
		}
	}
	if !inv.provable {
		return listing.LookupUnknown, nil, nil
	}
	ancestors := inv.ancestors(key)
	switch {
	case foundKind(found, ancestors, listing.KindUnknown):
		return listing.LookupUnknown, nil, errUnresolvedKind
	case foundKind(found, ancestors, listing.KindSymlink):
		return listing.LookupUnknown, nil, errThroughLink
	}
	return listing.LookupAbsent, nil, nil
}

// foundKind reports whether an entry found at one of keys is of kind k.
func foundKind(found map[string]*listing.Entry, keys [][]byte, k listing.Kind) bool {
	for _, key := range keys {
		if e := found[string(key)]; e != nil && e.Kind == k {
			return true
		}
	}
	return false
}

// entries reads the entries at keys, by their keys, in one planned scan.
func (inv *Inventory) entries(ctx context.Context, keys [][]byte) (map[string]*listing.Entry, error) {
	if len(keys) == 0 {
		return map[string]*listing.Entry{}, nil
	}
	keys = slices.Clone(keys)
	slices.SortFunc(keys, bytes.Compare)
	keys = slices.CompactFunc(keys, bytes.Equal)
	selections := make([]seekparquet.Selection, len(keys))
	for i, key := range keys {
		selections[i] = seekparquet.Selection{Span: seekparquet.KeySpan(key)}
	}

	plan, err := inv.sp.Plan(ctx, selections, inv.rows.columns, nil)
	if err != nil {
		return nil, err
	}
	return inv.read(ctx, plan)
}

// read reads the entries a plan with the view's projection selects, by
// their keys.
func (inv *Inventory) read(ctx context.Context, plan *seekparquet.Plan) (map[string]*listing.Entry, error) {
	found := map[string]*listing.Entry{}
	d := decoder{rows: inv.rows}
	var batch []listing.Entry
	err := plan.Scan(ctx, nil, func(b *seekparquet.Batch) error {
		var err error
		if batch, err = d.batchEntries(b, batch); err != nil {
			return err
		}
		for _, e := range batch {
			e.Key = bytes.Clone(e.Key)
			found[string(e.Key)] = &e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}
