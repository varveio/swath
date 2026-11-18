// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	spwrite "github.com/varveio/seekparquet/go/write"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// rec is one row of a fixture: the columns the view reads, and a local
// entry's kind. Every other column is null.
type rec struct {
	key      string
	size     *int64
	modified *time.Time
	class    *string
	kind     string
	links    *uint32
	inode    *uint64
	dev      *uint64
	alloc    *int64
	// extra are other columns' values.
	extra map[string]any
}

// columns is the row's values by column, for schema id.
func (r rec) columns(id reader.SchemaID) map[string]any {
	m := map[string]any{"key": r.key}
	for k, v := range r.extra {
		m[k] = v
	}
	put := func(name string, ok bool, v any) {
		if ok {
			m[name] = v
		}
	}
	put("size", r.size != nil, deref(r.size))
	put("last_modified", r.modified != nil, deref(r.modified))
	if id != reader.LocalV1 {
		put("storage_class", r.class != nil, deref(r.class))
		return m
	}
	m["kind"] = r.kind
	put("nlink", r.links != nil, deref(r.links))
	put("inode", r.inode != nil, deref(r.inode))
	put("dev", r.dev != nil, deref(r.dev))
	put("allocated_bytes", r.alloc != nil, deref(r.alloc))
	return m
}

func deref[T any](p *T) T {
	var v T
	if p != nil {
		v = *p
	}
	return v
}

func ptr[T any](v T) *T { return &v }

// base is the time of the fixtures' objects.
var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// objects are an object store's keys, as varve's swath fixture has them:
// an object that is also a prefix ("a/"), a name that sorts before a
// prefix of the same stem ("a" before "a/", "a/b" before "a/b0"), a wide
// prefix spread over pages and parts (bulk/), a deep one, and one with no
// time or class (nulls/).
func objects() []rec {
	at := func(ms int) *time.Time { return ptr(base.Add(time.Duration(ms) * time.Millisecond)) }
	rs := []rec{
		{key: "a", size: ptr[int64](1), modified: at(0), class: ptr("STANDARD")},
		{key: "a/", size: ptr[int64](0), modified: at(0), class: ptr("STANDARD")},
		{key: "a/b", size: ptr[int64](10), modified: at(1), class: ptr("STANDARD_IA")},
		{key: "a/b0", size: ptr[int64](100), modified: at(2), class: ptr("STANDARD")},
		{key: "a/c/d", size: ptr[int64](5), modified: at(3), class: ptr("STANDARD")},
		{key: "ab/x", size: ptr[int64](1000), modified: at(4), class: ptr("GLACIER")},
	}
	for i := range 300 {
		rs = append(rs, rec{key: fmt.Sprintf("bulk/%05d", i), size: ptr(int64(i + 1)), modified: at(i), class: ptr("STANDARD")})
	}
	return append(rs,
		rec{key: "deep/one/two/file.txt", size: ptr[int64](7), modified: at(0), class: ptr("STANDARD")},
		rec{key: "nulls/no-metadata", size: ptr[int64](3)},
		rec{key: "z", size: ptr[int64](9), modified: at(5), class: ptr("STANDARD")},
	)
}

// tree is a local tree as swath lists it: directory rows ending in "/",
// an empty directory, nested ones, a name that is not UTF-8, a symlink
// with its target, a FIFO and an entry of unknown kind, with every stat
// column.
func tree() []rec {
	entry := func(key, kind string, size, alloc int64, ino uint64) rec {
		return rec{key: key, kind: kind, size: ptr(size), alloc: ptr(alloc), modified: ptr(base.Add(time.Duration(ino) * time.Second)),
			links: ptr[uint32](1), inode: ptr(ino), dev: ptr[uint64](64)}
	}
	link := entry("link", "symlink", 9, 0, 9)
	link.extra = map[string]any{"link_target": "d"}
	return []rec{
		entry("a.txt", "file", 5, 4096, 1),
		entry("bad\xffname.txt", "file", 3, 4096, 2),
		entry("d/", "dir", 4096, 4096, 3),
		entry("d/b.txt", "file", 10, 4096, 4),
		entry("d/nested/", "dir", 4096, 4096, 5),
		entry("d/nested/c.bin", "file", 100, 0, 6),
		entry("empty/", "dir", 4096, 4096, 7),
		entry("fifo", "fifo", 0, 0, 8),
		link,
		entry("u", "unknown", 0, 0, 10),
		entry("z/", "dir", 4096, 4096, 11),
		entry("z/y/", "dir", 4096, 4096, 12),
		entry("z/y/f", "file", 7, 8192, 13),
	}
}

// namesOnly is a tree listed with --metadata names: every stat column
// null.
func namesOnly(rs []rec) []rec {
	out := make([]rec, len(rs))
	for i, r := range rs {
		out[i] = rec{key: r.key, kind: r.kind}
	}
	return out
}

// extension is extensions.swath of a complete, unfiltered listing of the
// whole source.
func extension(id reader.SchemaID) map[string]any {
	source := map[string]any{"kind": "s3", "uri": "s3://b", "container": "b"}
	switch id {
	case reader.GCSObjectsV1:
		source = map[string]any{"kind": "gcs", "uri": "gs://b", "container": "b"}
	case reader.AzureObjectsV1:
		source = map[string]any{"kind": "azure", "uri": "az://acct/b", "container": "b"}
	case reader.LocalV1:
		source = map[string]any{"kind": "local", "uri": "/data"}
	}
	return map[string]any{
		"version":      reader.MetadataVersion,
		"published_at": "2026-11-12T09:00:02Z",
		"source":       source,
		"scope":        map[string]any{"kind": "whole"},
		"coverage": map[string]any{"complete": true, "stop_reason": "end_of_scope", "unreadable": 0,
			"exclusions": []string{}, "filters": []any{},
			"listing_started_at": "2026-11-12T09:00:00Z", "listing_finished_at": "2026-11-12T09:00:01Z"},
	}
}

// coverage returns ext with its coverage members replaced by set's.
func coverage(ext map[string]any, set map[string]any) map[string]any {
	c := ext["coverage"].(map[string]any)
	for k, v := range set {
		c[k] = v
	}
	return ext
}

// fixture is a dataset to write.
type fixture struct {
	id   reader.SchemaID
	ext  map[string]any // extensions.swath; nil for extension(id)
	rows []rec
	// pageRows and partBytes shape the dataset; zero for 16 rows a page
	// and 4 KiB a part, so a few hundred rows span pages and parts.
	pageRows  int
	partBytes int64
}

// write writes f as a dataset with SeekParquet's writer, as swath's
// schema registry describes its columns, and returns its directory.
func write(t testing.TB, f fixture) string {
	t.Helper()
	spec, ok := reader.Schema(f.id)
	if !ok {
		t.Fatalf("no schema %s", f.id)
	}
	group := parquet.Group{}
	for _, c := range append([]reader.ColumnSpec{spec.Key}, spec.Columns...) {
		group[c.Name] = node(c)
	}
	schema := parquet.NewSchema("swath", group)
	leaves := schema.Columns()
	rows := make([]parquet.Row, len(f.rows))
	for i, r := range f.rows {
		values := r.columns(f.id)
		row := make(parquet.Row, len(leaves))
		for j, path := range leaves {
			c, _ := spec.Column(path[0])
			row[j] = value(t, c, values[path[0]], j)
		}
		rows[i] = row
	}

	ext := f.ext
	if ext == nil {
		ext = extension(f.id)
	}
	raw, err := json.Marshal(map[string]any{reader.Extension: ext})
	if err != nil {
		t.Fatal(err)
	}
	pageRows, partBytes := f.pageRows, f.partBytes
	if pageRows == 0 {
		pageRows = 16
	}
	if partBytes == 0 {
		partBytes = 4 << 10
	}
	dir := filepath.Join(t.TempDir(), "ds")
	err = local.WriteDir(context.Background(), dir, schema, &rowReader{rows: rows}, local.DatasetOptions{
		Part:          spwrite.PartOptions{Key: seekparquet.Key{Column: "key", Unique: spec.UniqueKeys}, SchemaID: string(f.id), PageRows: pageRows},
		PartBytes:     partBytes,
		Extensions:    raw,
		AllowUnsynced: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func node(c reader.ColumnSpec) parquet.Node {
	var n parquet.Node
	switch c.Type {
	case reader.TypeString:
		n = parquet.String()
	case reader.TypeBinary:
		n = parquet.Leaf(parquet.ByteArrayType)
	case reader.TypeBool:
		n = parquet.Leaf(parquet.BooleanType)
	case reader.TypeInt32:
		n = parquet.Int(32)
	case reader.TypeUint32:
		n = parquet.Uint(32)
	case reader.TypeInt64:
		n = parquet.Int(64)
	case reader.TypeUint64:
		n = parquet.Uint(64)
	case reader.TypeTimestampMicros:
		n = parquet.Timestamp(parquet.Microsecond)
	}
	if !c.Required {
		n = parquet.Optional(n)
	}
	return n
}

// value is column c's value v (nil for null) as leaf column j's.
func value(t testing.TB, c reader.ColumnSpec, v any, j int) parquet.Value {
	t.Helper()
	if v == nil {
		if c.Required {
			t.Fatalf("no value for the required column %s", c.Name)
		}
		return parquet.NullValue().Level(0, 0, j)
	}
	var pv parquet.Value
	switch x := v.(type) {
	case string:
		pv = parquet.ByteArrayValue([]byte(x))
	case int64:
		pv = parquet.Int64Value(x)
	case uint32:
		pv = parquet.Int32Value(int32(x))
	case uint64:
		pv = parquet.Int64Value(int64(x))
	case time.Time:
		pv = parquet.Int64Value(x.UnixMicro())
	case bool:
		pv = parquet.BooleanValue(x)
	default:
		t.Fatalf("column %s: a %T value", c.Name, v)
	}
	def := 1
	if c.Required {
		def = 0
	}
	return pv.Level(0, def, j)
}

type rowReader struct{ rows []parquet.Row }

func (r *rowReader) ReadRows(out []parquet.Row) (int, error) {
	n := copy(out, r.rows)
	r.rows = r.rows[n:]
	if len(r.rows) == 0 {
		return n, io.EOF
	}
	return n, nil
}

// open opens the dataset at dir, checks its parts against the schema, as
// an application admitting it would, and returns its inventory.
func open(t testing.TB, dir string) *Inventory {
	t.Helper()
	return openWith(t, dir, nil)
}

// openWith opens the dataset at dir through src, a Source of its files,
// or OpenDir when src is nil.
func openWith(t testing.TB, dir string, src seekparquet.Source) *Inventory {
	t.Helper()
	var d *reader.Dataset
	var err error
	if src == nil {
		d, err = reader.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	} else {
		d, err = reader.Open(context.Background(), src, seekparquet.OpenOptions{})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	inv, err := Open(d)
	if err != nil {
		t.Fatal(err)
	}
	return inv
}

// slowSource serves a dataset's files, each read of a part delay late,
// so that a call's time budget ends while it reads.
type slowSource struct {
	root  *os.Root
	delay time.Duration
}

func newSlowSource(t testing.TB, dir string, delay time.Duration) *slowSource {
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return &slowSource{root: root, delay: delay}
}

func (s *slowSource) Open(_ context.Context, name string) (seekparquet.SourceFile, error) {
	f, err := s.root.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	delay := s.delay
	if name == seekparquet.ManifestName {
		delay = 0
	}
	return slowFile{File: f, size: info.Size(), delay: delay}, nil
}

type slowFile struct {
	*os.File
	size  int64
	delay time.Duration
}

func (f slowFile) Size() int64 { return f.size }

func (f slowFile) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(f.delay)
	return f.File.ReadAt(p, off)
}

// child is an expected child of a level: its key and kind.
type child struct {
	key  string
	kind listing.Kind
}

// children lists the level of prefix from the records alone,
// independently of SeekParquet's delimited listing: every key one level
// down, and every deeper prefix once, in byte order; and the record at
// exactly the prefix.
func children(rs []rec, prefix string, isLocal bool) (self *rec, out []child) {
	seen := map[string]bool{}
	for i, r := range rs {
		rest, ok := strings.CutPrefix(r.key, prefix)
		switch {
		case !ok:
		case rest == "":
			self = &rs[i]
		case strings.Contains(rest, "/"):
			key := prefix + rest[:strings.IndexByte(rest, '/')+1]
			kind := listing.KindPrefix
			if isLocal {
				kind = listing.KindDir
			}
			if !seen[key] {
				seen[key] = true
				out = append(out, child{key, kind})
			}
		default:
			out = append(out, child{r.key, kindOf(r, isLocal)})
		}
	}
	slices.SortFunc(out, func(a, b child) int { return strings.Compare(a.key, b.key) })
	return self, out
}

// kindOf is the kind of a record's Entry, as the contract maps the kinds
// swath writes: independently of the view's own map.
func kindOf(r rec, isLocal bool) listing.Kind {
	if !isLocal {
		return listing.KindObject
	}
	switch r.kind {
	case "file":
		return listing.KindFile
	case "dir":
		return listing.KindDir
	case "symlink":
		return listing.KindSymlink
	case "unknown":
		return listing.KindUnknown
	}
	return listing.KindOther
}

// totalsOf counts the records of kinds (files and objects for none)
// selected by keep, as the contract says: nil sums when a counted record
// lacks the value.
func totalsOf(rs []rec, isLocal bool, kinds []listing.Kind, keep func(rec) bool) listing.Totals {
	if len(kinds) == 0 {
		kinds = []listing.Kind{listing.KindFile, listing.KindObject}
	}
	t := listing.Totals{Bytes: new(int64), AllocatedBytes: new(int64)}
	dated := true
	for _, r := range rs {
		if !keep(r) || !slices.Contains(kinds, kindOf(r, isLocal)) {
			continue
		}
		t.Entries++
		add := func(sum **int64, v *int64) {
			if *sum != nil && v != nil {
				**sum += *v
			} else {
				*sum = nil
			}
		}
		add(&t.Bytes, r.size)
		alloc := r.alloc
		if !isLocal {
			alloc = nil
		}
		add(&t.AllocatedBytes, alloc)
		switch {
		case r.modified == nil:
			dated = false
		case t.Newest == nil || r.modified.After(*t.Newest):
			t.Newest = ptr(*r.modified)
		}
	}
	if !dated {
		t.Newest = nil
	}
	return t
}

func sameTotals(a, b listing.Totals) bool {
	eq := func(x, y *int64) bool { return x == nil && y == nil || x != nil && y != nil && *x == *y }
	sameTime := a.Newest == nil && b.Newest == nil || a.Newest != nil && b.Newest != nil && a.Newest.Equal(*b.Newest)
	return a.Entries == b.Entries && eq(a.Bytes, b.Bytes) && eq(a.AllocatedBytes, b.AllocatedBytes) && sameTime
}

func showTotals(t listing.Totals) string {
	show := func(p *int64) string {
		if p == nil {
			return "nil"
		}
		return fmt.Sprint(*p)
	}
	newest := "nil"
	if t.Newest != nil {
		newest = t.Newest.Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("{%d entries, %s bytes, %s allocated, newest %s}", t.Entries, show(t.Bytes), show(t.AllocatedBytes), newest)
}

// sameEntry reports whether e is r's entry.
func sameEntry(e listing.Entry, r rec, isLocal bool) bool {
	eq64 := func(x, y *int64) bool { return x == nil && y == nil || x != nil && y != nil && *x == *y }
	sameTime := e.LastModified == nil && r.modified == nil || e.LastModified != nil && r.modified != nil && e.LastModified.Equal(*r.modified)
	ok := bytes.Equal(e.Key, []byte(r.key)) && e.Kind == kindOf(r, isLocal) && eq64(e.Size, r.size) && sameTime
	if !isLocal {
		return ok && (e.StorageClass == nil) == (r.class == nil) && (r.class == nil || *e.StorageClass == *r.class) &&
			e.AllocatedBytes == nil && e.Inode == nil && e.Device == nil && e.Links == nil && e.LinkTarget == nil
	}
	target, linked := r.extra["link_target"].(string)
	ok = ok && (e.LinkTarget != nil) == linked && string(e.LinkTarget) == target
	return ok && eq64(e.AllocatedBytes, r.alloc) && e.StorageClass == nil &&
		deref(e.Inode) == deref(r.inode) && deref(e.Device) == deref(r.dev) && deref(e.Links) == deref(r.links) &&
		(e.Inode == nil) == (r.inode == nil)
}
