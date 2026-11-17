// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"context"
	"math"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// Page returns the next children of req.Prefix, one level by "/", in byte
// order, from SeekParquet's delimited listing (Dataset.Page). An object's
// child prefix is a KindPrefix entry; a local child directory is its own
// row, a KindDir. The entry at exactly the prefix comes back as Self on
// the level's first page. A token is bound to the inventory's manifest
// digest and to the prefix: one issued for another is ErrInvalidToken.
//
// A prefix outside the inventory's scope is ErrOutOfScope. A local
// directory the inventory has as another kind is ErrNotDirectory; one it
// lacks, ErrNotFound when it proves the absence, and an empty page
// otherwise. req.MaxScanned never stops it: it reads the children it
// returns. When req.MaxDuration ends the page it holds no entry, and its
// Next is req.Token, which starts the page again.
//
// With req.ChildTotals, each child prefix or directory gets the totals
// Summarize gives a subtree target at its key, from the rows the listing
// established, within ChildTotals.Budget and the page's MaxDuration:
// totals the budget does not reach are nil, and leave the page complete.
// They check the values they read, not the order of the keys.
func (inv *Inventory) Page(ctx context.Context, req listing.PageRequest) (listing.Page, error) {
	sel, err := inv.checkPage(req)
	if err != nil {
		return listing.Page{}, err
	}
	if !inv.inScope(req.Prefix) {
		return listing.Page{}, errOutOfScope
	}
	cursor, err := inv.decodeToken(req.Prefix, req.Token)
	if err != nil {
		return listing.Page{}, err
	}

	op, cancel := withBudget(ctx, req.MaxDuration)
	defer cancel()
	first := req.Token == ""
	limit := req.Limit
	if first && len(req.Prefix) > 0 {
		// One more, which is Self when the prefix has an entry.
		limit++
	}
	listed, err := inv.sp.Page(op, seekparquet.PageQuery{ListQuery: seekparquet.ListQuery{
		Prefix:    req.Prefix,
		Delimiter: '/',
		Cursor:    cursor,
		Limit:     limit,
		Columns:   inv.rows.columns,
	}}, nil)
	if err != nil {
		return inv.stopped(ctx, op, req.Token, err)
	}

	page := listing.Page{Order: listing.OrderBytes, Evidence: inv.evidence(listing.StopCompleted)}
	entries, next := listed.Entries, listed.Next
	if limit > req.Limit {
		if page.Self, entries, next, err = inv.self(req.Prefix, req.Limit, entries, next); err != nil {
			return listing.Page{}, err
		}
	}
	page.Entries = make([]listing.Entry, len(entries))
	for i := range entries {
		e, err := inv.listedEntry(entries[i].ListEntry)
		if err != nil {
			return listing.Page{}, err
		}
		page.Entries[i] = *e
	}

	if first && len(page.Entries) == 0 && page.Self == nil && inv.rows.local && len(req.Prefix) > 0 {
		if err := inv.missingDirectory(op, req.Prefix); err != nil {
			return inv.stopped(ctx, op, req.Token, err)
		}
	}
	if req.ChildTotals != nil {
		if page.Totals, err = inv.childTotals(ctx, op, entries, sel, req.ChildTotals.Budget); err != nil {
			return listing.Page{}, err
		}
	}
	if next != nil {
		if page.Next, err = inv.encodeToken(req.Prefix, next); err != nil {
			return listing.Page{}, err
		}
	}
	return page, nil
}

// stopped is the page err ended: at the page's time budget, a page with
// no entry whose Next is token, which starts it again; else the error, or
// ctx's once ctx has ended.
func (inv *Inventory) stopped(ctx, op context.Context, token string, err error) (listing.Page, error) {
	if timeUp(ctx, op) {
		return listing.Page{Order: listing.OrderBytes, Next: token, Evidence: inv.evidence(listing.StopTimeBudget)}, nil
	}
	return listing.Page{}, ctxOr(ctx, err)
}

// self takes the entry at exactly prefix from the first page of its
// level, listed with one entry more than limit for it: Self when the
// first entry is it, else the page cut to limit, continuing after its
// last entry.
func (inv *Inventory) self(prefix []byte, limit int, entries []seekparquet.PageEntry, next *seekparquet.Cursor) (*listing.Entry, []seekparquet.PageEntry, *seekparquet.Cursor, error) {
	if len(entries) > 0 && entries[0].Row != nil && bytes.Equal(entries[0].Row.Key, prefix) {
		self, err := inv.listedEntry(entries[0].ListEntry)
		return self, entries[1:], next, err
	}
	if len(entries) > limit {
		entries = entries[:limit]
		next = entries[limit-1].Cursor()
	}
	return nil, entries, next, nil
}

// checkPage refuses a request this package does not serve, and returns
// the kinds its child totals count.
func (inv *Inventory) checkPage(req listing.PageRequest) (selection, error) {
	if err := inv.checkPrefix(req.Prefix); err != nil {
		return selection{}, err
	}
	switch {
	case req.Limit < 1 || req.Limit >= math.MaxInt-1:
		return selection{}, invalid("a page's Limit is positive")
	case req.MaxScanned < 0 || req.MaxDuration < 0:
		return selection{}, invalid("a negative bound")
	case req.ChildTotals == nil:
		return selection{}, nil
	}
	if err := checkBudget(req.ChildTotals.Budget); err != nil {
		return selection{}, err
	}
	return selectionOf(req.ChildTotals.Kinds)
}

// listedEntry is one entry of a delimited listing, the caller's to keep.
// An object store's common prefix is a KindPrefix entry; a local one is
// its directory, from the directory's own row when the listing paired it
// (ListEntry.Exact), else its key alone.
func (inv *Inventory) listedEntry(le seekparquet.ListEntry) (*listing.Entry, error) {
	d := decoder{rows: inv.rows}
	switch {
	case le.Row != nil:
		e, err := d.entry(ownedRow(le.Row))
		return &e, err
	case !inv.rows.local:
		return prefixEntry(le.CommonPrefix, listing.KindPrefix)
	case le.Exact != nil:
		e, err := d.entry(ownedRow(le.Exact))
		return &e, err
	}
	return prefixEntry(le.CommonPrefix, listing.KindDir)
}

// prefixEntry is the entry of a common prefix with no row of its own, a
// key the listing made from its rows' keys: checked as a row's key is.
func prefixEntry(key []byte, kind listing.Kind) (*listing.Entry, error) {
	if bytes.IndexByte(key, 0) >= 0 {
		return nil, errNULKey
	}
	return &listing.Entry{Key: key, Kind: kind}, nil
}

// missingDirectory explains a local directory key with no row and nothing
// under it: ErrNotDirectory when the inventory has its name as another
// kind, ErrNotFound when the inventory proves it absent, else nothing.
func (inv *Inventory) missingDirectory(ctx context.Context, prefix []byte) error {
	name := prefix[:len(prefix)-1]
	found, err := inv.entries(ctx, [][]byte{name})
	switch {
	case err != nil:
		return err
	case found[string(name)] != nil:
		return errNotDirectory
	case inv.provable:
		return errNotFound
	}
	return nil
}

var (
	errNotDirectory = wrap(listing.ErrNotDirectory, "the inventory lists the name as another kind")
	errNotFound     = wrap(listing.ErrNotFound, "the inventory has no such directory")
)

// childTotals are the totals of each child prefix or directory of a
// page, aligned with listed: nil for any other entry, and for one whose
// rows the listing did not establish or the budget did not reach. Their
// time budget is the budget's and op's, the page's; ctx is the caller's.
func (inv *Inventory) childTotals(ctx, op context.Context, listed []seekparquet.PageEntry, sel selection, budget listing.Budget) ([]*listing.Totals, error) {
	out := make([]*listing.Totals, len(listed))
	rows := rowBudget{max: budget.MaxEntries}
	var counted []int
	var spans []seekparquet.RowSpan
	for i, le := range listed {
		if le.CommonPrefix == nil {
			continue
		}
		if !established(le.Rows) {
			continue
		}
		if fits, err := rows.admit([]seekparquet.RowSpan{le.Rows}); err != nil || !fits {
			continue
		}
		counted = append(counted, i)
		spans = append(spans, le.Rows)
	}
	if len(spans) == 0 {
		return out, nil
	}

	totalsCtx, cancel := withBudget(op, budget.MaxDuration)
	defer cancel()
	totals, err := inv.aggregate(totalsCtx, spans, sel, valueBudget(budget), false)
	if err != nil {
		if ctx.Err() == nil && totalsCtx.Err() != nil {
			return out, nil
		}
		return nil, ctxOr(ctx, err)
	}
	for j, i := range counted {
		out[i] = totals[j]
	}
	return out, nil
}

// ctxOr is ctx's error once ctx has ended, else err.
func ctxOr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
