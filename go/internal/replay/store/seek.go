// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/replay/legacy"
	"github.com/varveio/swath/go/objfile"
)

// ErrRepeatedKeys refuses a SeekParquet dataset whose keys repeat (a
// versions listing, --all-versions): the routes page by key alone, so a
// page boundary inside a key's group would lose the rest of the group.
// They are served once the versions routes carry a key's position in its
// group (issue #279).
var ErrRepeatedKeys = errors.New("replay does not serve datasets with repeated keys yet")

// reader is a fixture's sorted reader: the legacy core over a Java-era
// fixture (legacyReader), or seekReader over a SeekParquet
// dataset. Both take the legacy core's requests and return its rows.
type reader interface {
	Read(ctx context.Context, rng legacy.Range, stats *legacy.Stats) ([]legacy.Row, error)
	Scan(ctx context.Context, rng legacy.Range, stats *legacy.Stats, sink columnSink) error
	RollupRead(ctx context.Context, rl legacy.Rollup, stats *RollupStats) ([]legacy.Entry, error)
	FirstKeys() [][]byte
	Columns() []string
	Parts() int
	RowGroups() int
	Rows() int64
	Close() error
}

var _ reader = legacyReader{}

// columnSink receives a reader's Scan: the legacy core hands it parquet-go
// page values (legacy.ColumnSink.Values), the SeekParquet reader the
// module's borrowed column views (View), each straight from the decoded
// page, neither copying a value nor building a row.
type columnSink interface {
	legacy.ColumnSink
	// View gives the values of column c (an index into Range.Columns) for
	// the run's rows, in row order, in one call: a view of a column the
	// run's part has (seekparquet.ColumnView.Present), of the column's
	// declared type (legacy.CheckColumns). The view and its bytes are
	// valid only until the call returns: a sink copies what it keeps.
	View(c int, col seekparquet.ColumnView)
}

// manifestHead is the part of a manifest that tells the formats apart.
type manifestHead struct {
	Format string `json:"format"`
}

// isSeekParquet reports whether dir holds a manifest declaring the
// SeekParquet format (a dataset this build writes).
func isSeekParquet(dir string) bool {
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return false
	}
	var head manifestHead
	return json.Unmarshal(b, &head) == nil && head.Format == seekparquet.Format
}

// seekReader serves a SeekParquet dataset through the seekparquet module:
// its range reads and its delimited listing. Keys must be unique
// (ErrRepeatedKeys). A read's page work is the module's per-request
// seekparquet.ReadStats, added to the read's legacy.Stats (count), and a
// rollup's use of the list index to its RollupStats (countIndex).
type seekReader struct {
	d       *seekparquet.Dataset
	root    *os.Root
	meta    seekparquet.Metadata
	first   [][]byte
	columns []string
	groups  int
}

// cachedSource opens a dataset's files through a FileCache, so the parts
// hold at most its bound of descriptors whatever their number; root
// confines the first open of each to the dataset. It keeps the files it
// opened, by path, for the footer checks after the module's open.
type cachedSource struct {
	root   *os.Root
	dir    string
	files  *legacy.FileCache
	mu     sync.Mutex
	opened map[string]*legacy.CachedFile
}

func (s *cachedSource) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	f, err := s.files.Open(ctx, filepath.Join(s.dir, filepath.FromSlash(path)), func() (*os.File, error) {
		return s.root.OpenFile(path, legacy.OpenFlags, 0)
	})
	if errors.Is(err, legacy.ErrNotRegular) {
		// A FIFO or device at a part's path is not a part (as OpenDir).
		return nil, fmt.Errorf("%s: %w", path, fs.ErrNotExist)
	}
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.opened[path] = f
	s.mu.Unlock()
	return f, nil
}

// openSeek opens the SeekParquet dataset in dir, its parts through files,
// checking every part, every row group's first key page and each part's
// last at open (Dataset.Verify, as the legacy core decodes every row
// group's first key at open: a long-lived local reader fails fast rather
// than on a part's first touch), and the type of every column of expect a part has
// (legacy.CheckColumns).
func openSeek(ctx context.Context, dir string, expect []legacy.Column, files *legacy.FileCache) (*seekReader, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	src := &cachedSource{root: root, dir: dir, files: files, opened: map[string]*legacy.CachedFile{}}
	d, err := seekparquet.OpenDataset(ctx, src, seekparquet.OpenOptions{})
	if err == nil {
		if err = d.Verify(ctx, nil); err != nil {
			_ = d.Close()
		}
	}
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	r := &seekReader{d: d, root: root, meta: d.Metadata()}
	if !r.meta.Key.Unique {
		_ = r.Close()
		return nil, fmt.Errorf("%w: the manifest declares key.unique false (%s)", ErrRepeatedKeys, r.meta.SchemaID)
	}
	if err := r.inspect(src, expect); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

// inspect reads every part's footer: the column checks, the value columns
// (the first part's order, then later parts' additions), the non-empty
// row groups and their first keys, from the key chunk's minimum (exact in
// a SeekParquet part, whose key bounds are its first and last keys).
func (r *seekReader) inspect(src *cachedSource, expect []legacy.Column) error {
	seen := map[string]bool{}
	for _, part := range r.meta.Parts {
		f := src.opened[part.Path]
		if f == nil {
			return fmt.Errorf("%s: not opened", part.Path)
		}
		footer, err := legacy.OpenFooter(f, f.Size())
		if err != nil {
			return fmt.Errorf("%s: %w", part.Path, err)
		}
		if err := legacy.CheckColumns(part.Path, footer, expect); err != nil {
			return err
		}
		key, _ := footer.Schema().Lookup(r.meta.Key.Column)
		for _, path := range footer.Schema().Columns() {
			if len(path) != 1 || path[0] == r.meta.Key.Column || seen[path[0]] {
				continue
			}
			if leaf, _ := footer.Schema().Lookup(path...); leaf.MaxRepetitionLevel == 0 {
				seen[path[0]] = true
				r.columns = append(r.columns, path[0])
			}
		}
		for i, rg := range footer.Metadata().RowGroups {
			if rg.NumRows == 0 {
				continue
			}
			r.groups++
			first := rg.Columns[key.ColumnIndex].MetaData.Statistics.MinValue
			if len(first) == 0 || i == 0 {
				first = part.MinKey
			}
			r.first = append(r.first, bytes.Clone(first))
		}
	}
	return nil
}

func (r *seekReader) FirstKeys() [][]byte {
	out := make([][]byte, len(r.first))
	for i, k := range r.first {
		out[i] = bytes.Clone(k)
	}
	return out
}

func (r *seekReader) Columns() []string { return append([]string(nil), r.columns...) }
func (r *seekReader) Parts() int        { return len(r.meta.Parts) }
func (r *seekReader) RowGroups() int    { return r.groups }

func (r *seekReader) Rows() int64 {
	var n int64
	for _, p := range r.meta.Parts {
		n += p.Rows
	}
	return n
}

func (r *seekReader) Close() error {
	err := r.d.Close()
	return errors.Join(err, r.root.Close())
}

// rows converts the module's rows to the legacy core's: an absent value is
// a null one, as the legacy core returns a column a part lacks.
func rows(in []seekparquet.Row) []legacy.Row {
	out := make([]legacy.Row, len(in))
	for i := range in {
		out[i] = row(&in[i])
	}
	return out
}

func row(in *seekparquet.Row) legacy.Row {
	values := make([]parquet.Value, len(in.Values))
	for j, v := range in.Values {
		values[j] = legacyValue(v)
	}
	return legacy.Row{Key: in.Key, Columns: in.Columns, Values: values}
}

// legacyValue returns v as the legacy core's value, referencing v's bytes:
// a null for a null or an absent value (the legacy core returns a column a
// part lacks as null).
func legacyValue(v seekparquet.Value) parquet.Value {
	switch v.Kind() {
	case seekparquet.KindBoolean:
		return parquet.BooleanValue(v.Boolean())
	case seekparquet.KindInt32:
		return parquet.Int32Value(v.Int32())
	case seekparquet.KindInt64:
		return parquet.Int64Value(v.Int64())
	case seekparquet.KindInt96:
		return parquet.Int96Value(deprecated.Int96(v.Int96()))
	case seekparquet.KindFloat:
		return parquet.FloatValue(v.Float())
	case seekparquet.KindDouble:
		return parquet.DoubleValue(v.Double())
	case seekparquet.KindByteArray:
		return parquet.ByteArrayValue(v.Bytes())
	case seekparquet.KindFixedLenByteArray:
		return parquet.FixedLenByteArrayValue(v.Bytes())
	}
	return parquet.NullValue()
}

// after is the row cursor past every row of key's group (an offset beyond
// a group's last row continues at the next key, PAGE-1).
func after(key []byte) *seekparquet.Cursor { return seekparquet.RowCursor(key, math.MaxInt64) }

// count adds the module's counts of one request to stats: pages (key and
// value), keys decoded, row groups opened, page-index seeks, groups
// skipped whole, and parts checked on their first touch.
func count(stats *legacy.Stats, st *seekparquet.ReadStats) {
	if stats == nil {
		return
	}
	stats.PagesDecoded += st.KeyPagesDecoded + st.ValuePagesDecoded
	stats.KeysDecoded += st.KeysDecoded
	stats.GroupsOpened += st.RowGroupsOpened
	stats.Reseeks += st.PageIndexSeeks
	stats.GroupShortcuts += st.GroupsSkipped
	stats.PartsOpened += st.PartsOpened
}

func (r *seekReader) Read(ctx context.Context, rng legacy.Range, stats *legacy.Stats) ([]legacy.Row, error) {
	if rng.Limit <= 0 {
		return nil, nil
	}
	var st seekparquet.ReadStats
	defer count(stats, &st)
	out, err := r.d.Rows(ctx, seekQuery(rng), &st)
	if err != nil {
		return nil, err
	}
	return rows(out), nil
}

// seekQuery is the module's request for rng, of a positive Limit.
func seekQuery(rng legacy.Range) seekparquet.Query {
	span := seekparquet.Span{Start: rng.From, StartExclusive: rng.From != nil && !rng.FromInclusive, End: rng.To}
	return seekparquet.Query{Selection: seekparquet.Selection{Span: span}, Limit: int64(rng.Limit), Columns: rng.Columns}
}

// Scan reads the rows Read returns and hands them to sink straight from
// the module's borrowed batches (Dataset.Scan), one run per batch, without
// cloning a value or building a row: the keys and column views are valid
// only until each sink call returns, a column a part has is one View, and
// one it lacks is Missing.
func (r *seekReader) Scan(ctx context.Context, rng legacy.Range, stats *legacy.Stats, sink columnSink) error {
	if rng.Limit <= 0 {
		return nil
	}
	var st seekparquet.ReadStats
	defer count(stats, &st)
	var keys [][]byte
	return r.d.Scan(objfile.WithCacheMode(ctx, objfile.CacheBypass), seekQuery(rng), &st, func(b *seekparquet.Batch) error {
		keys = keys[:0]
		for i := range b.Len() {
			keys = append(keys, b.Key(i))
		}
		sink.Keys(keys)
		for c := range rng.Columns {
			if col := b.Column(c); col.Present() {
				sink.View(c, col)
			} else {
				sink.Missing(c)
			}
		}
		clear(keys) // hold no borrowed key past the callback
		return nil
	})
}

// RollupRead answers rl with the module's delimited listing, which has no
// start bound: a start is a row cursor past its group (exclusive), or the
// lookup of its own group followed by that cursor (inclusive). With
// SuppressAtOrBeforeFrom, a start that rolls up to a common prefix
// continues after that prefix (a prefix cursor): the only common prefix at
// or before the start that a later key can roll up to is the start's own,
// and every key under it is skipped with it.
func (r *seekReader) RollupRead(ctx context.Context, rl legacy.Rollup, stats *RollupStats) ([]legacy.Entry, error) {
	if rl.Limit <= 0 {
		return nil, nil
	}
	from, prefix := rl.From, rl.Prefix
	list := seekparquet.ListQuery{Prefix: prefix, Delimiter: rl.Delimiter, End: rl.To, Limit: rl.Limit, Columns: rl.Columns}
	if c := bytes.Compare(from, prefix); from == nil || c < 0 || c == 0 && rl.FromInclusive {
		// Every candidate key begins with the prefix, so is at or after
		// the start.
		return r.list(ctx, list, nil, stats)
	}
	if !bytes.HasPrefix(from, prefix) || rl.To != nil && bytes.Compare(from, rl.To) >= 0 {
		// After every key that begins with the prefix, or at or past the
		// end.
		return nil, nil
	}
	var rolled []byte
	if i := bytes.IndexByte(from[len(prefix):], rl.Delimiter); i >= 0 {
		rolled = from[:len(prefix)+i+1]
	}
	switch {
	case rolled != nil && rl.SuppressAtOrBeforeFrom:
		list.Cursor = seekparquet.PrefixCursor(rolled)
		return r.list(ctx, list, nil, stats)
	case !rl.FromInclusive:
		list.Cursor = after(from)
		return r.list(ctx, list, nil, stats)
	}
	// An inclusive start inside the prefix: its own group first.
	limit, columns := rl.Limit, rl.Columns
	if rolled != nil {
		limit, columns = 1, nil
	}
	var st seekparquet.ReadStats
	lookup := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan(from)}, Limit: int64(limit), Columns: columns}
	group, err := r.d.Rows(ctx, lookup, &st)
	count(&stats.Stats, &st)
	if err != nil {
		return nil, err
	}
	var lead []legacy.Entry
	switch {
	case len(group) == 0:
	case rolled != nil:
		lead = []legacy.Entry{{CommonPrefix: bytes.Clone(rolled)}}
	default:
		for i := range group {
			converted := row(&group[i])
			lead = append(lead, legacy.Entry{Row: &converted})
		}
		if len(lead) == rl.Limit {
			return lead, nil
		}
		list.Limit -= len(lead)
	}
	list.Cursor = after(from)
	return r.list(ctx, list, lead, stats)
}

// list runs req and appends its entries to lead; a common prefix lead
// ends with is not repeated, and the result keeps at most the request's
// limit of entries after lead's rows.
func (r *seekReader) list(ctx context.Context, req seekparquet.ListQuery, lead []legacy.Entry, stats *RollupStats) ([]legacy.Entry, error) {
	var st seekparquet.ReadStats
	entries, err := r.d.List(ctx, req, &st)
	count(&stats.Stats, &st)
	countIndex(&stats.ListIndex, &st)
	if err != nil {
		return nil, err
	}
	out := lead
	for i, e := range entries {
		if e.Row == nil {
			if i == 0 && len(lead) > 0 && bytes.Equal(lead[len(lead)-1].CommonPrefix, e.CommonPrefix) && lead[len(lead)-1].Row == nil {
				continue
			}
			out = append(out, legacy.Entry{CommonPrefix: e.CommonPrefix})
			continue
		}
		converted := row(e.Row)
		out = append(out, legacy.Entry{Row: &converted})
	}
	if len(lead) == 1 && lead[0].Row == nil && len(out) > req.Limit {
		out = out[:req.Limit]
	}
	return out, nil
}
