// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"context"
	"slices"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// Lookup says what is at exactly key: present when the inventory has a row
// for it; absent only inside the inventory's scope and when its Unlocated
// is 0, as of the listing; otherwise unknown. A key outside the scope is
// unknown with ErrOutOfScope as its Reason, and a read that fails leaves
// it unknown with the failure as its Reason. On a local inventory a key
// without a trailing "/" is also looked up as a directory's key. An empty
// key, or one holding a NUL, is ErrInvalidRequest. It returns an error only
// for such a request, and when ctx ends.
func (inv *Inventory) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	if err := inv.checkKey(key); err != nil {
		return listing.LookupResult{}, err
	}
	result := listing.LookupResult{Evidence: inv.evidence(listing.StopCompleted)}
	if !inv.inScope(key) {
		result.Reason = errOutOfScope
		return result, nil
	}

	found, err := inv.entries(ctx, inv.names(key))
	if err != nil {
		if ctx.Err() != nil {
			return listing.LookupResult{}, ctx.Err()
		}
		result.Reason = err
		return result, nil
	}
	result.State, result.Entry = inv.resolve(key, found)
	return result, nil
}

// names are the keys an entry at key may have: key, and on a local
// inventory the directory key "key/" of a key without a trailing "/".
func (inv *Inventory) names(key []byte) [][]byte {
	if !inv.rows.local || key[len(key)-1] == '/' {
		return [][]byte{key}
	}
	return [][]byte{key, append(bytes.Clone(key), '/')}
}

// resolve answers what is at key from the entries found at its names: the
// entry at key itself before a directory's.
func (inv *Inventory) resolve(key []byte, found map[string]*listing.Entry) (listing.LookupState, *listing.Entry) {
	for _, name := range inv.names(key) {
		if e := found[string(name)]; e != nil {
			return listing.LookupPresent, cloneEntry(e)
		}
	}
	if inv.provable {
		return listing.LookupAbsent, nil
	}
	return listing.LookupUnknown, nil
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
