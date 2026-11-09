// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// localRow is a swath/local-entries/v1 row as swath writes it.
type localRow struct {
	Key               []byte  `parquet:"key"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	Kind              string  `parquet:"kind"`
	Mode              *uint32 `parquet:"mode,optional"`
	UID               *uint32 `parquet:"uid,optional"`
	GID               *uint32 `parquet:"gid,optional"`
	Nlink             *uint32 `parquet:"nlink,optional"`
	Inode             *uint64 `parquet:"inode,optional"`
	Dev               *uint64 `parquet:"dev,optional"`
	AllocatedBytes    *int64  `parquet:"allocated_bytes,optional"`
	MetadataChangedAt *int64  `parquet:"metadata_changed_at,optional,timestamp(microsecond:utc)"`
	Atime             *int64  `parquet:"atime,optional,timestamp(microsecond:utc)"`
	BirthTime         *int64  `parquet:"birth_time,optional,timestamp(microsecond:utc)"`
	LinkTarget        []byte  `parquet:"link_target,optional"`
}

type treeEntry struct {
	key, kind string
	size      int64
	target    string
}

// localTree is a tree as swath lists it: directory rows ending in "/" (d/e/
// empty), a name that is not UTF-8, a symlink, a FIFO and nested
// directories, in key order.
var localTree = []treeEntry{
	{"a.txt", "FILE", 3, ""},
	{"d/", "DIR", 4096, ""},
	{"d/e/", "DIR", 4096, ""},
	{"d/link", "SYMLINK", 8, "../a.txt"},
	{"d/\xff", "FILE", 0, ""},
	{"fifo", "FIFO", 0, ""},
	{"z/", "DIR", 4096, ""},
	{"z/y/", "DIR", 4096, ""},
	{"z/y/f", "FILE", 7, ""},
}

// localDir writes localTree over two parts cut inside d/, with every stat
// column, or, names, with the stat columns null as --metadata names
// writes them.
func localDir(t *testing.T, names bool) string {
	t.Helper()
	rows := make([]localRow, len(localTree))
	for i, e := range localTree {
		rows[i] = localRow{Key: []byte(e.key), Kind: e.kind}
		if e.target != "" {
			rows[i].LinkTarget = []byte(e.target)
		}
		if !names {
			rows[i].Size, rows[i].LastModified = ptr(e.size), ptr(int64(1_700_000_000_000_000+i))
			rows[i].Mode, rows[i].Inode = ptr(uint32(0o644)), ptr(uint64(100+i))
		}
	}
	ext := swathExtension()
	ext["source"] = map[string]any{"kind": "local-fs", "uri": "/data"}
	s := spec{id: LocalV1, key: s3Spec().key, ext: ext}
	dir := writeGeometry(t, s, rows[:3], 2, 2)
	appendPart(t, dir, writeGeometry(t, s, rows[3:], 2, 2))
	return dir
}

// wantLocal is localTree's entry at key as StreamLocal decodes it.
func wantLocal(key string, names bool) LocalEntry {
	i := slices.IndexFunc(localTree, func(e treeEntry) bool { return e.key == key })
	e := localTree[i]
	w := LocalEntry{Position: Position{Key: []byte(key)}, Kind: e.kind}
	if e.target != "" {
		w.LinkTarget = ptr([]byte(e.target))
	}
	if !names {
		w.Size, w.LastModified = ptr(e.size), ptr(time.UnixMicro(int64(1_700_000_000_000_000+i)).UTC())
		w.Mode, w.Inode = ptr(uint32(0o644)), ptr(uint64(100+i))
	}
	return w
}

func streamLocal(t *testing.T, d *Dataset) []LocalEntry {
	t.Helper()
	var out []LocalEntry
	if err := d.StreamLocal(context.Background(), func(e LocalEntry) error {
		out = append(out, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestStreamLocal: every entry of every kind, with every column and its
// nulls, its key and target bytes exact.
func TestStreamLocal(t *testing.T) {
	for _, names := range []bool{false, true} {
		got := streamLocal(t, openDir(t, localDir(t, names)))
		if len(got) != len(localTree) {
			t.Fatalf("names %t: %d entries, want %d", names, len(got), len(localTree))
		}
		for i, e := range got {
			if want := wantLocal(localTree[i].key, names); !reflect.DeepEqual(e, want) {
				t.Fatalf("names %t: entry %d %+v, want %+v", names, i, e, want)
			}
		}
	}
}

// TestLocalAndObjectsRefused: Stream and Scan read objects and refuse a
// filesystem's entries; StreamLocal refuses every other schema; nothing is
// read before refusing.
func TestLocalAndObjectsRefused(t *testing.T) {
	d := openDir(t, localDir(t, false))
	if _, err := stream(t, d); !errors.Is(err, ErrSchema) {
		t.Fatalf("stream entries: %v", err)
	}
	var st ReadStats
	if err := d.Scan(context.Background(), ScanRequest{Stats: &st}, func([]Object) error { return nil }); !errors.Is(err, ErrSchema) || st != (ReadStats{}) {
		t.Fatalf("scan entries: %v, %+v", err, st)
	}
	o := openDir(t, writeDataset(t, s3Spec(), s3Rows(3)))
	if err := o.StreamLocal(context.Background(), func(LocalEntry) error { return nil }); !errors.Is(err, ErrSchema) {
		t.Fatalf("stream objects as entries: %v", err)
	}
	if st := d.StreamStats(); st.Requests != 0 {
		t.Fatalf("read before refusing: %+v", st)
	}
}

// describeLocal returns a listing entry of a filesystem dataset as
// "o:key", "p:prefix", or "D:prefix" for a directory with its own row.
func describeLocal(e Entry) string {
	switch {
	case e.Object != nil:
		return "object!"
	case e.CommonPrefix == nil:
		return "o:" + string(e.Local.Key)
	case e.Local == nil:
		return "p:" + string(e.CommonPrefix)
	case bytes.Equal(e.Local.Key, e.CommonPrefix):
		return "D:" + string(e.CommonPrefix)
	}
	return fmt.Sprintf("p:%s with the entry %q", e.CommonPrefix, e.Local.Key)
}

// TestListLocal: a listing of a filesystem's entries pairs each directory
// with its own row, an empty one included, keeps binary names, targets
// and nulls, and pages with each entry's cursor; Page adds the prefixes'
// totals, nil bytes where an entry has no size.
func TestListLocal(t *testing.T) {
	for _, names := range []bool{false, true} {
		d := openDir(t, localDir(t, names))
		for _, tc := range []struct {
			prefix string
			want   []string
		}{
			{"", []string{"o:a.txt", "D:d/", "o:fifo", "D:z/"}},
			{"d/", []string{"o:d/", "D:d/e/", "o:d/link", "o:d/\xff"}},
			{"z/", []string{"o:z/", "D:z/y/"}},
		} {
			for _, limit := range []int{1, 2, 100} {
				req := PageRequest{ListRequest: ListRequest{Prefix: []byte(tc.prefix), Delimiter: '/', Limit: limit},
					Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}}
				var got []string
				for {
					page, err := d.Page(context.Background(), req)
					if err != nil {
						t.Fatal(err)
					}
					for _, e := range page.Entries {
						got = append(got, describeLocal(e.Entry))
						if e.Local != nil && !reflect.DeepEqual(*e.Local, wantLocal(string(e.Local.Key), names)) {
							t.Fatalf("entry %+v, want %+v", *e.Local, wantLocal(string(e.Local.Key), names))
						}
						if e.CommonPrefix == nil {
							continue
						}
						var count, size int64
						for _, x := range localTree {
							if strings.HasPrefix(x.key, string(e.CommonPrefix)) {
								count, size = count+1, size+x.size
							}
						}
						if a := e.Aggregate; *a.Count != count || names != (a.Bytes == nil) || a.Bytes != nil && *a.Bytes != size {
							t.Fatalf("names %t: %q: %s, want count %d, bytes %d", names, e.CommonPrefix, describeAggregate(*a), count, size)
						}
					}
					if req.Cursor = page.Next; req.Cursor == nil {
						break
					}
				}
				if strings.Join(got, " ") != strings.Join(tc.want, " ") {
					t.Fatalf("names %t: %q in pages of %d: %q, want %q", names, tc.prefix, limit, got, tc.want)
				}
			}
		}
	}
}

// TestLocalTotals: totals of a filesystem's entries count every kind, a
// directory's own row included once for its path and the root's entries
// below it, sum their st_size, directories' included, and leave the bytes
// and newest time nil, never 0, when the entries were listed without a
// stat.
func TestLocalTotals(t *testing.T) {
	paths := []string{"", "d/", "d/e/", "z/", "z/y/f", "d/link", "a.txt", "missing/", "d"}
	for _, names := range []bool{false, true} {
		d := openDir(t, localDir(t, names))
		var objects []Object
		for _, e := range streamLocal(t, d) {
			objects = append(objects, Object{Position: e.Position, Size: e.Size, LastModified: e.LastModified})
		}
		checkTotals(t, d, objects, paths)
		got, err := d.Totals(context.Background(), []string{"", "d/"})
		if err != nil {
			t.Fatal(err)
		}
		root, dir := got[0], got[1]
		switch {
		case root.Count != 9 || dir.Count != 4 || string(dir.Exact.Key) != "d/":
			t.Fatalf("names %t: counts %d and %d, exact %+v", names, root.Count, dir.Count, dir.Exact)
		case names && (root.Bytes != nil || dir.Bytes != nil || root.NewestModified != nil || dir.Exact.Size != nil):
			t.Fatalf("names: %s, %s: want nil bytes and times", describeTotals(root), describeTotals(dir))
		case !names && (*root.Bytes != 4*4096+3+8+7 || *dir.Bytes != 2*4096+8):
			t.Fatalf("stat: %s, %s", describeTotals(root), describeTotals(dir))
		}
	}
}
