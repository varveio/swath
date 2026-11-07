// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/parquet-go/parquet-go"
)

// Stats counts the work of reads. A read adds to the Stats it is given;
// the caller owns synchronization.
type Stats struct {
	// PagesDecoded counts data pages decoded, key and value columns.
	PagesDecoded int64
	// KeysDecoded counts key values decoded from key pages.
	KeysDecoded int64
	// GroupsOpened counts key cursors opened on a row group.
	GroupsOpened int64
	// Reseeks counts key cursor moves that skipped at least one page of
	// their row group through the page index instead of decoding it.
	Reseeks int64
	// GroupShortcuts counts row groups a rollup skipped whole because their
	// first key and the next group's share one common prefix.
	GroupShortcuts int64
}

// keyCursor walks the key column of one row group forward, holding the
// page it decoded last.
type keyCursor struct {
	ctx      context.Context
	part     *part
	physical int
	index    *groupIndex
	pages    *parquet.FilePages
	page     parquet.Page
	values   []parquet.Value
	at       int // index of the decoded page, -1 before the first
	next     int // position within values
	last     []byte
	stats    *Stats
}

func (p *part) keyCursor(ctx context.Context, physical int, index *groupIndex, stats *Stats) *keyCursor {
	if stats == nil {
		stats = new(Stats)
	}
	return &keyCursor{ctx: ctx, part: p, physical: physical, index: index, at: -1, stats: stats}
}

// seek returns the first key at or after from (after it when !inclusive)
// from the cursor's position on, and its row, or ok=false when the row
// group holds none. The key aliases the decoded page until the cursor moves
// to another page or closes.
func (c *keyCursor) seek(from []byte, inclusive bool) (key []byte, row int64, ok bool, err error) {
	for {
		for ; c.next < len(c.values); c.next++ {
			key := c.values[c.next].ByteArray()
			if from == nil {
				return key, c.index.pages[c.at].first + int64(c.next), true, nil
			}
			if order := bytes.Compare(key, from); order > 0 || order == 0 && inclusive {
				return key, c.index.pages[c.at].first + int64(c.next), true, nil
			}
		}
		target := c.at + 1
		if from != nil {
			if page := c.index.pageFor(target, from); page > target {
				if c.at >= 0 {
					c.stats.Reseeks++
				}
				target = page
			}
		}
		if target >= len(c.index.pages) {
			return nil, 0, false, nil
		}
		if err := c.decode(target); err != nil {
			return nil, 0, false, err
		}
	}
}

// advance moves past the current key and returns the next one, as seek.
func (c *keyCursor) advance() (key []byte, row int64, ok bool, err error) {
	c.next++
	return c.seek(nil, true)
}

// decode reads key page i and checks it against its index entry and the
// keys decoded before it.
func (c *keyCursor) decode(i int) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if c.pages == nil {
		chunk, err := c.part.chunk(c.physical, c.part.key)
		if err != nil {
			return err
		}
		// SeekToRow uses the OffsetIndex only once the chunk has loaded it,
		// which keyIndex did.
		c.pages = chunk.PagesFrom(c.part.at(c.ctx))
	}
	if c.page != nil {
		// The previous page's last key must outlive its buffers.
		c.last = append(c.last[:0], c.values[len(c.values)-1].ByteArray()...)
		parquet.Release(c.page)
		c.page, c.values = nil, c.values[:0]
	}
	meta := c.index.pages[i]
	page, values, err := readPage(c.pages, meta.first, c.values)
	if err != nil {
		return fmt.Errorf("%s row group %d key page %d: %w", c.part.name, c.physical, i, err)
	}
	c.page, c.values, c.at, c.next = page, values, i, 0
	c.stats.PagesDecoded++
	c.stats.KeysDecoded += int64(len(values))
	if int64(len(values)) != meta.rows {
		return corrupt("%s row group %d key page %d holds %d rows, its index %d", c.part.name, c.physical, i, len(values), meta.rows)
	}
	for j, value := range values {
		key := value.ByteArray()
		if value.IsNull() || len(key) == 0 {
			return corrupt("%s row group %d key page %d holds an empty key", c.part.name, c.physical, i)
		}
		previous := c.last
		if j > 0 {
			previous = values[j-1].ByteArray()
		}
		if (j > 0 || c.last != nil) && bytes.Compare(key, previous) <= 0 {
			return rowDisorder(c.part.name, c.physical, meta.first+int64(j), key)
		}
	}
	if bytes.Compare(values[0].ByteArray(), meta.lo) < 0 || !meta.holdsLast(values[len(values)-1].ByteArray()) {
		return corrupt("%s row group %d key page %d holds a key outside its index bounds", c.part.name, c.physical, i)
	}
	return nil
}

func (c *keyCursor) close() {
	if c.page != nil {
		parquet.Release(c.page)
		c.page = nil
	}
	if c.pages != nil {
		_ = c.pages.Close()
		c.pages = nil
	}
	c.values = nil
}

// readPage seeks pages to row and decodes the page holding it, from row to
// the page's end, into values (which then alias the page's buffers).
func readPage(pages *parquet.FilePages, row int64, values []parquet.Value) (parquet.Page, []parquet.Value, error) {
	if err := pages.SeekToRow(row); err != nil {
		return nil, nil, fmt.Errorf("seek to row %d: %w", row, err)
	}
	page, err := pages.ReadPage()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, fmt.Errorf("read page at row %d: %w", row, err)
	}
	values, err = pageValues(page, values)
	if err != nil {
		parquet.Release(page)
		return nil, nil, err
	}
	if len(values) == 0 {
		parquet.Release(page)
		return nil, nil, fmt.Errorf("page at row %d holds no values", row)
	}
	return page, values, nil
}

// pageValues decodes every value of a page of a non-repeated column.
func pageValues(page parquet.Page, values []parquet.Value) ([]parquet.Value, error) {
	count := int(page.NumValues())
	if page.NumRows() != int64(count) {
		return nil, errors.New("page values do not match its rows")
	}
	values = slices.Grow(values[:0], count)[:count]
	reader := page.Values()
	for read := 0; read < count; {
		n, err := reader.ReadValues(values[read:])
		read += n
		if read == count {
			break
		}
		if errors.Is(err, io.EOF) || err == nil && n == 0 {
			return nil, errors.New("page holds fewer values than it declares")
		}
		if err != nil {
			return nil, fmt.Errorf("read page values: %w", err)
		}
	}
	return values, nil
}

// readValues reads n rows of the named value columns of a physical row
// group from row on, cloned out of the page buffers; only those columns'
// pages are decoded. A column the part lacks is nil (null).
func (p *part) readValues(ctx context.Context, physical int, row int64, n int, names []string, stats *Stats) ([][]parquet.Value, error) {
	out := make([][]parquet.Value, len(names))
	for i, name := range names {
		column, ok := p.leaves[name]
		if !ok {
			continue
		}
		values, err := p.readColumn(ctx, physical, column, row, n, stats)
		if err != nil {
			return nil, fmt.Errorf("%s row group %d column %s: %w", p.name, physical, name, err)
		}
		out[i] = values
	}
	return out, nil
}

func (p *part) readColumn(ctx context.Context, physical, column int, row int64, n int, stats *Stats) ([]parquet.Value, error) {
	chunk, err := p.chunk(physical, column)
	if err != nil {
		return nil, err
	}
	// Without an OffsetIndex SeekToRow decodes from the chunk's start,
	// which is correct, only slower.
	if _, err := chunk.OffsetIndexFrom(p.at(ctx)); err != nil && !errors.Is(err, parquet.ErrMissingOffsetIndex) {
		return nil, fmt.Errorf("read offset index: %w", err)
	}
	pages := chunk.PagesFrom(p.at(ctx))
	defer func() { _ = pages.Close() }()
	out := make([]parquet.Value, 0, n)
	var scratch []parquet.Value
	for next := row; len(out) < n; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, values, err := readPage(pages, next, scratch)
		if err != nil {
			return nil, err
		}
		stats.PagesDecoded++
		take := min(len(values), n-len(out))
		for _, value := range values[:take] {
			out = append(out, value.Clone())
		}
		parquet.Release(page)
		scratch = values[:0]
		next += int64(take)
	}
	return out, nil
}
