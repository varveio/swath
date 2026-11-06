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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// KeyColumn is the name of the column every part is sorted by.
const KeyColumn = "key"

// PartSource is one part file, in key order among the parts given to Open.
type PartSource struct {
	// Name identifies the part in errors and logs.
	Name string
	// File reads the part; it must be safe for concurrent ReadAt calls (an
	// *os.File is). Close closes it when it implements io.Closer.
	File io.ReaderAt
	// Size is the part's length in bytes.
	Size int64
	// Footer is the part's parsed footer when a profile has already read it
	// with OpenFooter; nil reads it.
	Footer *parquet.File
	// MinKey and MaxKey are the part's exact first and last keys when its
	// profile records them (a manifest does); Open decodes both and refuses
	// the set with ReasonSanityFailed when they differ. Nil checks nothing.
	MinKey, MaxKey []byte
	// BoundBytes is the length at which the key column's ColumnIndex bounds
	// may have been truncated by the writer: a bound this long or longer is
	// treated as conservative. It must be positive.
	BoundBytes int
}

// Column is a type expectation for a value column, by name: a part that
// has the column must hold it as a non-repeated leaf of Kind that Check (when
// set) accepts, or the set is refused with ReasonUnsupportedSchema. A part
// without the column is not refused; reads return it as null.
type Column struct {
	Name  string
	Kind  parquet.Kind
	Check func(node parquet.Node) error
}

// Options configures Open.
type Options struct {
	// Expect lists type expectations for value columns. Reads may project
	// any column by name whether or not it is listed here.
	Expect []Column
	// CheckGroup, when set, is called for every non-empty row group in key
	// order, with its part's footer, before that group's first key is
	// checked; an error refuses the set. A profile uses it for decisions it
	// takes from footer metadata (row types, sorting columns).
	CheckGroup func(part string, footer *parquet.File, physical int, group *format.RowGroup) error
}

// Reader serves bounded, ordered reads over a fixed set of sorted parts. It
// is safe for concurrent use: the parsed footers and page indexes are shared
// and immutable once loaded, and each read holds its own page readers.
type Reader struct {
	parts   []*part
	groups  []group
	columns []string
	rows    int64

	mu     sync.RWMutex
	closed bool
}

// part is one opened part: its parsed footer, located columns and the key
// page index of each row group, decoded once on first use.
type part struct {
	name   string
	file   io.ReaderAt
	size   int64
	footer *parquet.File
	bound  int
	key    int
	leaves map[string]int // non-repeated value leaves by name
	index  []atomic.Pointer[groupIndex]
}

// group is one non-empty row group in global key order.
type group struct {
	part     int
	physical int
	first    []byte
	rows     int64
}

// OpenFooter parses a part's footer without its page indexes or bloom
// filters; the reader loads the key page index of a row group when it is
// first read.
func OpenFooter(file io.ReaderAt, size int64) (*parquet.File, error) {
	f, err := parquet.OpenFile(file, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("open Parquet footer: %w", err)
	}
	return f, nil
}

// Open opens sources, which must be in key order, decoding the first key of
// every non-empty row group to build the routing index. The set is refused
// with a *RefusalError when it cannot be served sorted; other errors report
// I/O or malformed files.
func Open(ctx context.Context, sources []PartSource, opt Options) (*Reader, error) {
	r := &Reader{}
	var previous []byte
	for i, src := range sources {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := openPart(src, opt.Expect)
		if err != nil {
			return nil, err
		}
		r.parts = append(r.parts, p)
		r.columns = mergeColumns(r.columns, p.footer.Schema())
		first, last := len(r.groups), -1
		for physical := range p.footer.Metadata().RowGroups {
			rg := &p.footer.Metadata().RowGroups[physical]
			if rg.NumRows == 0 {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if opt.CheckGroup != nil {
				if err := opt.CheckGroup(src.Name, p.footer, physical, rg); err != nil {
					return nil, err
				}
			}
			chunk := rg.Columns[p.key]
			if chunk.ColumnIndexOffset <= 0 || chunk.ColumnIndexLength <= 0 || chunk.OffsetIndexOffset <= 0 || chunk.OffsetIndexLength <= 0 {
				return nil, Refuse(ReasonNoPageIndex, "%s row group %d: key column lacks a page index", src.Name, physical)
			}
			key, err := p.firstKey(physical)
			if err != nil {
				return nil, fmt.Errorf("%s row group %d: %w", src.Name, physical, err)
			}
			if previous != nil && bytes.Compare(previous, key) >= 0 {
				return nil, Refuse(ReasonSanityFailed, "row-group first keys not strictly ascending at %s row group %d", src.Name, physical)
			}
			r.groups = append(r.groups, group{part: i, physical: physical, first: key, rows: rg.NumRows})
			r.rows += rg.NumRows
			previous, last = key, len(r.groups)-1
		}
		if err := checkBounds(ctx, r, p, src, first, last); err != nil {
			return nil, err
		}
		if last >= 0 && src.MaxKey != nil {
			// The next part's first key must follow this part's last.
			previous = src.MaxKey
		}
	}
	return r, nil
}

// checkBounds compares a part's decoded first and last keys with the exact
// bounds its profile recorded.
func checkBounds(ctx context.Context, r *Reader, p *part, src PartSource, first, last int) error {
	if src.MinKey == nil && src.MaxKey == nil {
		return nil
	}
	if last < 0 {
		return Refuse(ReasonSanityFailed, "%s holds no rows but records key bounds", src.Name)
	}
	if src.MinKey != nil && !bytes.Equal(r.groups[first].first, src.MinKey) {
		return Refuse(ReasonSanityFailed, "%s: decoded first key differs from its recorded minimum", src.Name)
	}
	if src.MaxKey != nil {
		key, err := p.lastKey(ctx, r.groups[last].physical)
		if err != nil {
			return fmt.Errorf("%s: %w", src.Name, err)
		}
		if !bytes.Equal(key, src.MaxKey) {
			return Refuse(ReasonSanityFailed, "%s: decoded last key differs from its recorded maximum", src.Name)
		}
	}
	return nil
}

func openPart(src PartSource, expect []Column) (*part, error) {
	if src.File == nil || src.BoundBytes <= 0 {
		return nil, fmt.Errorf("sortedparquet: part %s: a file and a positive bound length are required", src.Name)
	}
	footer := src.Footer
	if footer == nil {
		var err error
		if footer, err = OpenFooter(src.File, src.Size); err != nil {
			return nil, fmt.Errorf("%s: %w", src.Name, err)
		}
	}
	if err := checkRowCounts(footer.Metadata()); err != nil {
		return nil, fmt.Errorf("%s: %w", src.Name, err)
	}
	p := &part{name: src.Name, file: src.File, size: src.Size, footer: footer, bound: src.BoundBytes}
	key, ok := footer.Schema().Lookup(KeyColumn)
	if !ok || !key.Node.Leaf() || key.Node.Type().Kind() != parquet.ByteArray || !key.Node.Required() {
		return nil, Refuse(ReasonUnsupportedSchema, "%s: the key column must be a required byte array", src.Name)
	}
	p.key = key.ColumnIndex
	p.leaves = make(map[string]int)
	for _, path := range footer.Schema().Columns() {
		if len(path) != 1 || path[0] == KeyColumn {
			continue
		}
		leaf, _ := footer.Schema().Lookup(path...)
		if leaf.MaxRepetitionLevel == 0 {
			p.leaves[path[0]] = leaf.ColumnIndex
		}
	}
	for _, column := range expect {
		leaf, ok := footer.Schema().Lookup(column.Name)
		if !ok {
			continue
		}
		if !leaf.Node.Leaf() || leaf.Node.Repeated() || leaf.Node.Type().Kind() != column.Kind || leaf.MaxRepetitionLevel != 0 {
			return nil, Refuse(ReasonUnsupportedSchema, "%s: column %s is not a non-repeated %s", src.Name, column.Name, column.Kind)
		}
		if column.Check != nil {
			if err := column.Check(leaf.Node); err != nil {
				return nil, Refuse(ReasonUnsupportedSchema, "%s: column %s: %v", src.Name, column.Name, err)
			}
		}
	}
	p.index = make([]atomic.Pointer[groupIndex], len(footer.Metadata().RowGroups))
	return p, nil
}

// checkRowCounts checks that the row groups hold non-negative counts that
// sum to the footer's row count.
func checkRowCounts(metadata *format.FileMetaData) error {
	var total int64
	for _, rg := range metadata.RowGroups {
		if rg.NumRows < 0 || total+rg.NumRows < total {
			return errors.New("row group row counts are invalid")
		}
		total += rg.NumRows
	}
	if total != metadata.NumRows {
		return errors.New("row groups do not hold the footer's row count")
	}
	return nil
}

// chunk returns the file-backed column chunk of leaf column in physical
// row group.
func (p *part) chunk(physical, column int) (*parquet.FileColumnChunk, error) {
	chunk, ok := p.footer.RowGroups()[physical].ColumnChunks()[column].(*parquet.FileColumnChunk)
	if !ok {
		return nil, errors.New("column chunk is not file-backed")
	}
	return chunk, nil
}

// firstKey decodes the first key of a non-empty row group from its first
// key page.
func (p *part) firstKey(physical int) ([]byte, error) {
	chunk, err := p.chunk(physical, p.key)
	if err != nil {
		return nil, err
	}
	pages := chunk.PagesFrom(p.file)
	defer func() { _ = pages.Close() }()
	page, err := pages.ReadPage()
	if err != nil {
		return nil, fmt.Errorf("read first key page: %w", err)
	}
	defer parquet.Release(page)
	values := make([]parquet.Value, 1)
	n, err := page.Values().ReadValues(values)
	if n == 0 {
		if err == nil || errors.Is(err, io.EOF) {
			err = errors.New("empty key page")
		}
		return nil, fmt.Errorf("read first key: %w", err)
	}
	if values[0].IsNull() || len(values[0].ByteArray()) == 0 {
		return nil, Refuse(ReasonSanityFailed, "%s row group %d: first key is empty", p.name, physical)
	}
	return bytes.Clone(values[0].ByteArray()), nil
}

// lastKey decodes the last key of a non-empty row group from its last key
// page, located through the OffsetIndex.
func (p *part) lastKey(ctx context.Context, physical int) ([]byte, error) {
	index, err := p.keyIndex(physical)
	if err != nil {
		return nil, err
	}
	cursor := p.keyCursor(ctx, physical, index, nil)
	defer cursor.close()
	if err := cursor.decode(len(index.pages) - 1); err != nil {
		return nil, err
	}
	return bytes.Clone(cursor.values[len(cursor.values)-1].ByteArray()), nil
}

// Parts returns the number of parts.
func (r *Reader) Parts() int { return len(r.parts) }

// RowGroups returns the number of non-empty row groups.
func (r *Reader) RowGroups() int { return len(r.groups) }

// Rows returns the number of rows in the non-empty row groups.
func (r *Reader) Rows() int64 { return r.rows }

// Columns returns the names of the top-level, non-repeated value columns of
// the parts, in the first part's order then any later part's additions.
func (r *Reader) Columns() []string { return append([]string(nil), r.columns...) }

// mergeColumns adds the top-level non-repeated value columns of schema that
// names lacks.
func mergeColumns(names []string, schema *parquet.Schema) []string {
	for _, path := range schema.Columns() {
		if len(path) != 1 || path[0] == KeyColumn || slices.Contains(names, path[0]) {
			continue
		}
		if leaf, _ := schema.Lookup(path...); leaf.MaxRepetitionLevel == 0 {
			names = append(names, path[0])
		}
	}
	return names
}

// FirstKeys returns a copy of the decoded first key of every non-empty row
// group, in key order: the routing index.
func (r *Reader) FirstKeys() [][]byte {
	keys := make([][]byte, len(r.groups))
	for i, g := range r.groups {
		keys[i] = bytes.Clone(g.first)
	}
	return keys
}

// groupFor returns the row group that holds from: the last one whose first
// key is at or before it, or the first group.
func (r *Reader) groupFor(from []byte) int {
	if from == nil {
		return 0
	}
	n := sort.Search(len(r.groups), func(i int) bool { return bytes.Compare(r.groups[i].first, from) > 0 })
	return max(n-1, 0)
}

// enter takes a read lease that Close waits for.
func (r *Reader) enter() error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ErrClosed
	}
	return nil
}

func (r *Reader) leave() { r.mu.RUnlock() }

// Close waits for reads in progress and closes every part file that
// implements io.Closer. Reads after Close fail with ErrClosed.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var errs []error
	for _, p := range r.parts {
		if closer, ok := p.file.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close %s: %w", p.name, err))
			}
		}
	}
	return errors.Join(errs...)
}
