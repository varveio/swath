// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// keyPage is one key page as its row group's page indexes describe it.
type keyPage struct {
	// lo is the ColumnIndex minimum: a truncated minimum is a prefix of the
	// page's first key, so lo is never above it.
	lo []byte
	// hi is the ColumnIndex maximum. When truncated (hiMayBeTruncated) it
	// is either raised above the page's last key or, when every byte of the
	// cut prefix is 0xFF and cannot be raised, a prefix of it; so the last
	// key is then only known to sort below Successor(hi).
	hi               []byte
	hiMayBeTruncated bool
	first            int64
	rows             int64
}

// mayHoldAtOrAfter reports whether the page can hold a key at or after
// from, using its maximum only as far as truncation lets it bound the keys.
func (page *keyPage) mayHoldAtOrAfter(from []byte) bool {
	if from == nil {
		return true
	}
	if !page.hiMayBeTruncated {
		return bytes.Compare(page.hi, from) >= 0
	}
	ceiling, ok := Successor(page.hi)
	return !ok || bytes.Compare(from, ceiling) < 0
}

// holdsLast reports whether key may be the page's last key under its
// maximum.
func (page *keyPage) holdsLast(key []byte) bool {
	if !page.hiMayBeTruncated {
		return bytes.Compare(key, page.hi) <= 0
	}
	ceiling, ok := Successor(page.hi)
	return !ok || bytes.Compare(key, ceiling) < 0
}

// groupIndex is a row group's checked key page index.
type groupIndex struct {
	pages []keyPage
}

// pageFor returns a page at or after start, no later than the first page
// whose keys reach from, before which no page holds a key at or after from.
// The keys' real maxima ascend and mayHoldAtOrAfter is never false for a
// page that holds such a key, so the predicate is true from that page on;
// a truncated maximum may make it true earlier too, which costs only a page
// read.
func (index *groupIndex) pageFor(start int, from []byte) int {
	lo, hi := start, len(index.pages)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if index.pages[mid].mayHoldAtOrAfter(from) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// keyIndex returns the checked key page index of a non-empty physical row
// group, loading it on first use. Concurrent first uses may both load it;
// one result is kept.
func (p *part) keyIndex(physical int) (*groupIndex, error) {
	if index := p.index[physical].Load(); index != nil {
		return index, nil
	}
	chunk, err := p.chunk(physical, p.key)
	if err != nil {
		return nil, err
	}
	columnIndex, err := chunk.ColumnIndexFrom(p.file)
	if err != nil {
		return nil, fmt.Errorf("row group %d: read key column index: %w", physical, err)
	}
	offsets, err := chunk.OffsetIndexFrom(p.file)
	if err != nil {
		return nil, fmt.Errorf("row group %d: read key offset index: %w", physical, err)
	}
	index, err := checkKeyIndex(columnIndex, offsets, p.footer.Metadata().RowGroups[physical].NumRows, p.bound)
	if err != nil {
		return nil, fmt.Errorf("row group %d: %w", physical, err)
	}
	if !p.index[physical].CompareAndSwap(nil, index) {
		return p.index[physical].Load(), nil
	}
	return index, nil
}

// checkKeyIndex builds a row group's key pages from its ColumnIndex and
// OffsetIndex after checking that they describe the same pages, at least
// one, none null, with first rows starting at zero and ascending strictly
// below rows, minima that never descend, and exact maxima that never
// descend.
func checkKeyIndex(columns parquet.ColumnIndex, offsets parquet.OffsetIndex, rows int64, bound int) (*groupIndex, error) {
	n := columns.NumPages()
	if n == 0 || offsets.NumPages() != n {
		return nil, corrupt("key page indexes disagree on the page count")
	}
	index := &groupIndex{pages: make([]keyPage, n)}
	for i := range index.pages {
		if columns.NullPage(i) {
			return nil, corrupt("key page %d is null", i)
		}
		first := offsets.FirstRowIndex(i)
		end := rows
		if i+1 < n {
			end = offsets.FirstRowIndex(i + 1)
		}
		if i == 0 && first != 0 || end <= first {
			return nil, corrupt("key page %d row range is invalid", i)
		}
		lo := bytes.Clone(columns.MinValue(i).ByteArray())
		hi := bytes.Clone(columns.MaxValue(i).ByteArray())
		if bytes.Compare(lo, hi) > 0 {
			return nil, corrupt("key page %d bounds are inverted", i)
		}
		page := keyPage{lo: lo, hi: hi, hiMayBeTruncated: len(hi) >= bound, first: first, rows: end - first}
		if i > 0 {
			before := &index.pages[i-1]
			// A raised truncated maximum may sort above the next page's
			// exact one, so maxima are compared only when both are exact.
			if bytes.Compare(before.lo, lo) > 0 || !before.hiMayBeTruncated && !page.hiMayBeTruncated && bytes.Compare(before.hi, hi) > 0 {
				return nil, corrupt("key page %d bounds descend", i)
			}
		}
		index.pages[i] = page
	}
	return index, nil
}
