// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import (
	"bytes"
	"context"

	"github.com/parquet-go/parquet-go"
)

// Row is one row: its key and the values of the columns its read
// projected, by name. A null column, or one the row's part lacks, is a null
// Value. Rows own their bytes; Columns is shared by the rows of one read.
type Row struct {
	Key     []byte
	Columns []string
	Values  []parquet.Value
}

// Value returns the value of the named column, and false when the read did
// not project it.
func (row *Row) Value(name string) (parquet.Value, bool) {
	for i, column := range row.Columns {
		if column == name {
			return row.Values[i], true
		}
	}
	return parquet.Value{}, false
}

// Range is a bounded read: rows with key in [From, To) in ascending
// unsigned byte order, at most Limit of them. A nil From or To is open.
type Range struct {
	From          []byte
	FromInclusive bool
	To            []byte
	Limit         int
	// Columns names the value columns to decode for each row; no other
	// column's pages are read. Nil reads keys only.
	Columns []string
}

// Read returns the rows of rng. It decodes key pages only from the page
// that can hold From, and value pages only for the rows it returns.
func (r *Reader) Read(ctx context.Context, rng Range, stats *Stats) ([]Row, error) {
	if err := r.enter(); err != nil {
		return nil, err
	}
	defer r.leave()
	if stats == nil {
		stats = new(Stats)
	}
	if rng.Limit <= 0 || len(r.groups) == 0 {
		return nil, nil
	}
	var out []Row
	from, inclusive := rng.From, rng.FromInclusive
	var last []byte
	for g := r.groupFor(from); g < len(r.groups) && len(out) < rng.Limit; g++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		grp := r.groups[g]
		if rng.To != nil && bytes.Compare(grp.first, rng.To) >= 0 {
			break
		}
		rows, done, err := r.readGroup(ctx, grp, from, inclusive, rng, rng.Limit-len(out), last, stats)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if len(rows) > 0 {
			last = rows[len(rows)-1].Key
		}
		if done {
			break
		}
		// Later groups are read from their first row.
		from, inclusive = nil, true
	}
	return out, nil
}

// readGroup reads up to want rows of one row group at or after from; done
// reports that the range's upper bound was reached.
func (r *Reader) readGroup(ctx context.Context, grp group, from []byte, inclusive bool, rng Range, want int, last []byte, stats *Stats) ([]Row, bool, error) {
	p := r.parts[grp.part]
	index, err := p.keyIndex(grp.physical)
	if err != nil {
		return nil, false, err
	}
	cursor := p.keyCursor(ctx, grp.physical, index, stats)
	defer cursor.close()
	stats.GroupsOpened++
	key, row, ok, err := cursor.seek(from, inclusive)
	if err != nil {
		return nil, false, err
	}
	if ok && last != nil && bytes.Compare(key, last) <= 0 {
		return nil, false, corrupt("%s row group %d starts at or before the previous row group's keys", p.name, grp.physical)
	}
	start := row
	var keys [][]byte
	done := false
	for ok && len(keys) < want {
		if rng.To != nil && bytes.Compare(key, rng.To) >= 0 {
			done = true
			break
		}
		keys = append(keys, bytes.Clone(key))
		if len(keys) == want {
			break
		}
		key, _, ok, err = cursor.advance()
		if err != nil {
			return nil, false, err
		}
	}
	if len(keys) == 0 {
		return nil, done, nil
	}
	values, err := p.readValues(ctx, grp.physical, start, len(keys), rng.Columns, stats)
	if err != nil {
		return nil, false, err
	}
	rows := make([]Row, len(keys))
	for i := range keys {
		rows[i] = Row{Key: keys[i], Columns: rng.Columns, Values: rowValues(values, i)}
	}
	return rows, done, nil
}

// rowValues returns row i of column-major values, null where a column was
// not read.
func rowValues(values [][]parquet.Value, i int) []parquet.Value {
	row := make([]parquet.Value, len(values))
	for c := range row {
		if values[c] == nil {
			row[c] = parquet.NullValue()
			continue
		}
		row[c] = values[c][i]
	}
	return row
}

// Rollup is a delimited listing over [From, To) scoped to Prefix: the
// common prefixes keys roll up to at the first Delimiter after Prefix, and
// the rows directly under Prefix, interleaved in key order, at most Limit
// entries. From and To must lie inside Prefix's range.
type Rollup struct {
	From          []byte
	FromInclusive bool
	To            []byte
	Prefix        []byte
	Delimiter     byte
	Limit         int
	// SuppressAtOrBeforeFrom leaves out a common prefix that sorts at or
	// before From: a resume that lands inside a common prefix has already
	// been given it. Without it such a prefix is emitted when a key at or
	// after From lies under it.
	SuppressAtOrBeforeFrom bool
	// Columns names the value columns decoded for each object entry, as
	// Range.Columns.
	Columns []string
}

// Entry is one rollup entry: exactly one of CommonPrefix and Row is set.
type Entry struct {
	CommonPrefix []byte
	Row          *Row
}

// RollupRead answers rl as a skip-scan: each common prefix costs one routing
// hop and at most one page-index landing past it, never a decode of the
// keys under it, and a row group whose first key and the next group's first
// key share a common prefix is skipped without any read. Its entries are
// those a walk over Read would produce, one key at a time.
func (r *Reader) RollupRead(ctx context.Context, rl Rollup, stats *Stats) ([]Entry, error) {
	if err := r.enter(); err != nil {
		return nil, err
	}
	defer r.leave()
	if stats == nil {
		stats = new(Stats)
	}
	if rl.Limit <= 0 || len(r.groups) == 0 {
		return nil, nil
	}
	scan := rollupScan{r: r, rl: rl, stats: stats, cursorGroup: -1}
	defer scan.close()
	return scan.run(ctx)
}

type rollupScan struct {
	r           *Reader
	rl          Rollup
	stats       *Stats
	out         []Entry
	cursor      *keyCursor
	cursorGroup int
	values      valueRun
}

// valueRun holds the values of consecutive rows of one row group.
type valueRun struct {
	group  int
	first  int64
	values [][]parquet.Value
	n      int
}

func (s *rollupScan) close() {
	if s.cursor != nil {
		s.cursor.close()
	}
}

// emitPrefix adds a common prefix unless the floor rule suppresses it.
func (s *rollupScan) emitPrefix(prefix []byte) {
	if s.rl.SuppressAtOrBeforeFrom && s.rl.From != nil && bytes.Compare(prefix, s.rl.From) <= 0 {
		return
	}
	s.out = append(s.out, Entry{CommonPrefix: bytes.Clone(prefix)})
}

func (s *rollupScan) run(ctx context.Context) ([]Entry, error) {
	r, rl := s.r, s.rl
	cursor, inclusive := rl.From, rl.FromInclusive
	for len(s.out) < rl.Limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g := r.groupFor(cursor)
		grp := r.groups[g]
		if rl.To != nil && bytes.Compare(grp.first, rl.To) >= 0 {
			break
		}
		// A row group whose first key and the next group's first key share
		// a common prefix holds only keys under it: keys between two keys
		// that begin with a prefix begin with it too. Where the scan lands
		// inside the group, the next group's first key is a key at or after
		// it under the prefix, so the prefix is emitted without a read. When
		// the prefix is not suppressed at the floor, that witness must also
		// lie below To.
		if g+1 < len(r.groups) {
			whole := CommonPrefix(grp.first, rl.Prefix, rl.Delimiter)
			next := r.groups[g+1].first
			if whole != nil && bytes.Equal(whole, CommonPrefix(next, rl.Prefix, rl.Delimiter)) &&
				(rl.SuppressAtOrBeforeFrom || rl.To == nil || bytes.Compare(next, rl.To) < 0) {
				s.stats.GroupShortcuts++
				s.emitPrefix(whole)
				successor, ok := Successor(whole)
				if !ok {
					break
				}
				cursor, inclusive = successor, true
				continue
			}
		}
		if s.cursorGroup != g {
			if s.cursor != nil {
				s.cursor.close()
			}
			p := r.parts[grp.part]
			index, err := p.keyIndex(grp.physical)
			if err != nil {
				return nil, err
			}
			s.cursor = p.keyCursor(ctx, grp.physical, index, s.stats)
			s.cursorGroup = g
			s.stats.GroupsOpened++
		}
		key, row, ok, err := s.cursor.seek(cursor, inclusive)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Nothing at or after the cursor here: continue at the next
			// group's first key.
			if g+1 >= len(r.groups) {
				break
			}
			cursor, inclusive = r.groups[g+1].first, true
			continue
		}
		if rl.To != nil && bytes.Compare(key, rl.To) >= 0 {
			break
		}
		if prefix := CommonPrefix(key, rl.Prefix, rl.Delimiter); prefix != nil {
			s.emitPrefix(prefix)
			successor, ok := Successor(prefix)
			if !ok {
				break
			}
			cursor, inclusive = successor, true
			continue
		}
		key = bytes.Clone(key)
		values, i, err := s.valuesAt(ctx, g, row, rl.Limit-len(s.out))
		if err != nil {
			return nil, err
		}
		object := Row{Key: key, Columns: rl.Columns, Values: rowValues(values, i)}
		s.out = append(s.out, Entry{Row: &object})
		cursor, inclusive = key, false
	}
	return s.out, nil
}

// valuesAt returns the run of values holding row of group g and the row's
// position in it, reading up to want rows from row when the current run
// does not hold it: objects directly under a prefix are consecutive rows,
// so a run serves many of them with one read per column.
func (s *rollupScan) valuesAt(ctx context.Context, g int, row int64, want int) ([][]parquet.Value, int, error) {
	run := &s.values
	if run.values == nil || run.group != g || row < run.first || row >= run.first+int64(run.n) {
		grp := s.r.groups[g]
		n := int(min(int64(max(want, 1)), grp.rows-row))
		values, err := s.r.parts[grp.part].readValues(ctx, grp.physical, row, n, s.rl.Columns, s.stats)
		if err != nil {
			return nil, 0, err
		}
		*run = valueRun{group: g, first: row, values: values, n: n}
	}
	return run.values, int(row - run.first), nil
}
