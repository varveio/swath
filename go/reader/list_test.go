// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// listKeys holds a, a/, a/b, a/b0 and ab/x, whose order and rollup a
// listing by "/" must keep apart, cut into three parts below; a/ and c/3/
// are objects at a prefix's exact key, c/ is a prefix without one.
var listKeys = []string{"a", "a/", "a/b", "a/b0", "a/c/d", "ab/x", "b", "c/1", "c/2", "c/3/", "c/3/x", "d"}

// listDir writes listKeys over three parts, in pages of two rows and row
// groups of three, cutting inside a/ and c/ (a/ and its keys span parts 0
// and 1).
func listDir(t *testing.T) string {
	t.Helper()
	return writeParts(t, s3Spec(), 2, 3, cut(keyRows(listKeys), 0, 3, 8, len(listKeys))...)
}

// describe returns an entry as a string: "o:key", "p:prefix", or
// "P:prefix" for a prefix with the object at its exact key.
func describe(e Entry) string {
	switch {
	case e.CommonPrefix == nil:
		return "o:" + string(e.Object.Key)
	case e.Object == nil:
		return "p:" + string(e.CommonPrefix)
	case bytes.Equal(e.Object.Key, e.CommonPrefix):
		return "P:" + string(e.CommonPrefix)
	}
	return fmt.Sprintf("p:%s with the object %q", e.CommonPrefix, e.Object.Key)
}

// listOracle lists keys, sorted, by prefix and delimiter: a prefix whose
// first key is the prefix itself is "P:".
func listOracle(keys []string, prefix string, delim byte) []string {
	var out []string
	for _, k := range keys {
		rest, ok := strings.CutPrefix(k, prefix)
		if !ok {
			continue
		}
		if i := strings.IndexByte(rest, delim); i >= 0 {
			if p := prefix + rest[:i+1]; len(out) == 0 || out[len(out)-1][2:] != p {
				out = append(out, map[bool]string{false: "p:", true: "P:"}[p == k]+p)
			}
			continue
		}
		out = append(out, "o:"+k)
	}
	return out
}

// listAll pages through a listing with limit entries a page.
func listAll(t *testing.T, d *Dataset, req ListRequest) []string {
	t.Helper()
	var out []string
	for {
		page, err := d.List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return out
		}
		if len(page) > req.Limit {
			t.Fatalf("%d entries, limit %d", len(page), req.Limit)
		}
		for _, e := range page {
			if e.Object == nil && e.CommonPrefix == nil {
				t.Fatalf("entry %+v: neither Object nor CommonPrefix", e)
			}
			out = append(out, describe(e))
		}
		req.Cursor = page[len(page)-1].Cursor()
	}
}

// TestListPages: every page size, across parts and after objects and
// prefixes alike, lists what the oracle lists, in byte order.
func TestListPages(t *testing.T) {
	d := openDir(t, listDir(t))
	for _, prefix := range []string{"", "a", "a/", "c/", "zz"} {
		want := listOracle(listKeys, prefix, '/')
		for _, limit := range []int{1, 2, 3, 100} {
			t.Run(fmt.Sprintf("%q/%d", prefix, limit), func(t *testing.T) {
				if got := listAll(t, d, ListRequest{Prefix: []byte(prefix), Delimiter: '/', Limit: limit}); strings.Join(got, " ") != strings.Join(want, " ") {
					t.Fatalf("listed %q, want %q", got, want)
				}
			})
		}
	}
}

// TestListObjects: an object entry holds the row's decoded values.
func TestListObjects(t *testing.T) {
	rows := keyRows(listKeys)
	d := openDir(t, listDir(t))
	got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	o := got[0].Object
	if len(got) != 2 || o == nil || string(o.Key) != "a" || *o.Size != *rows[0].Size || *o.ETag != *rows[0].ETag ||
		*o.StorageClass != "STANDARD" || o.LastModified.UnixMicro() != *rows[0].LastModified || string(got[1].CommonPrefix) != "a/" {
		t.Fatalf("entries %+v", got)
	}
}

// TestListExactObject: a common prefix carries the object at its exact
// key, decoded as an object entry is, and its cursor stays the prefix's,
// so the next page starts after every key under it, that object
// included. A prefix without such an object, or reached after a row
// cursor inside its keys, carries none.
func TestListExactObject(t *testing.T) {
	rows := keyRows(listKeys)
	d := openDir(t, listDir(t))
	ctx := context.Background()
	got, err := d.List(ctx, ListRequest{Delimiter: '/', Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	e := got[1]
	o := e.Object
	if len(got) != 2 || string(e.CommonPrefix) != "a/" || o == nil || string(o.Key) != "a/" || o.Offset != 0 ||
		*o.Size != *rows[1].Size || *o.ETag != *rows[1].ETag || o.LastModified.UnixMicro() != *rows[1].LastModified {
		t.Fatalf("entries %+v, object %+v", got, o)
	}
	if c := e.Cursor(); c.Key != nil || string(c.Prefix) != "a/" {
		t.Fatalf("cursor of a prefix with its object: %+v", c)
	}
	next, err := d.List(ctx, ListRequest{Delimiter: '/', Cursor: e.Cursor(), Limit: 1})
	if err != nil || len(next) != 1 || describe(next[0]) != "p:ab/" {
		t.Fatalf("after a/: %+v, %v", next, err)
	}
	for _, tc := range []struct {
		prefix string
		cursor *Cursor
		want   string
	}{
		{"c/", nil, "o:c/1 o:c/2 P:c/3/"},
		{"", &Cursor{Key: []byte("a/")}, "p:a/ p:ab/ o:b p:c/ o:d"},
		{"", &Cursor{Key: []byte("c/2")}, "p:c/ o:d"},
	} {
		got := listAll(t, d, ListRequest{Prefix: []byte(tc.prefix), Delimiter: '/', Cursor: tc.cursor, Limit: 1})
		if strings.Join(got, " ") != tc.want {
			t.Fatalf("prefix %q after %+v: %q, want %q", tc.prefix, tc.cursor, got, tc.want)
		}
	}
}

// TestListExactObjectChecked: the object at a prefix's exact key is
// checked as any object is, and one that fails fails the listing.
func TestListExactObjectChecked(t *testing.T) {
	d := openDir(t, writeDataset(t, s3Spec(), []s3Row{{Key: "a/"}, {Key: "a/x", Size: ptr(int64(1))}}))
	got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 10})
	if se := semanticAt(t, err, KindRow, "size"); string(se.Row.Key) != "a/" || got != nil {
		t.Fatalf("%+v, %v", got, err)
	}
}

// TestListOwnsBytes: changing a returned key or prefix after taking its
// cursor changes neither the cursor nor the page that continues from it.
func TestListOwnsBytes(t *testing.T) {
	d := openDir(t, listDir(t))
	ctx := context.Background()
	for _, limit := range []int{1, 2} { // the last entry an object (a), then a prefix with its object (a/)
		page, err := d.List(ctx, ListRequest{Delimiter: '/', Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		last := page[len(page)-1]
		cursor := last.Cursor()
		if last.Object != nil {
			last.Object.Key[0] = 0xff
		}
		if last.CommonPrefix != nil {
			last.CommonPrefix[0] = 0xff
		}
		next, err := d.List(ctx, ListRequest{Delimiter: '/', Cursor: cursor, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if want := listOracle(listKeys, "", '/')[limit]; len(next) != 1 || describe(next[0]) != want {
			t.Fatalf("after %d entries: %+v, want %s", limit, next, want)
		}
	}
}

// TestListRefusesVersions: refused before any read, with the request's
// statistics reset from the work a previous request left in them.
func TestListRefusesVersions(t *testing.T) {
	s := s3Spec()
	s.id, s.key.Unique = S3VersionsV1, false
	d, src := openSource(t, writeDataset(t, s, s3Rows(3)))
	src.reset()
	st := ReadStats{PartsOpened: 3, KeysDecoded: 7, SourceReads: 9}
	if got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 10, Stats: &st}); !errors.Is(err, ErrSchema) || got != nil {
		t.Fatalf("list versions: %+v, %v", got, err)
	}
	if st != (ReadStats{}) {
		t.Fatalf("stats after a refusal before any read: %+v", st)
	}
	if _, reads := src.taken(); len(reads) != 0 {
		t.Fatalf("%d reads before refusing", len(reads))
	}
}

// TestListRowChecks: an object Stream refuses fails the listing, with no
// entries, and the request's statistics are kept.
func TestListRowChecks(t *testing.T) {
	d := openDir(t, writeDataset(t, s3Spec(), []s3Row{{Key: "a", Size: ptr(int64(1))}, {Key: "b"}, {Key: "c/d", Size: ptr(int64(1))}}))
	var st ReadStats
	got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 10, Stats: &st})
	se := semanticAt(t, err, KindRow, "size")
	if string(se.Row.Key) != "b" || got != nil {
		t.Fatalf("%+v, %v", got, err)
	}
	if st.KeysDecoded == 0 || st.ValuePagesDecoded == 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestListStats(t *testing.T) {
	d := openDir(t, listDir(t))
	var st ReadStats
	if _, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 100, Stats: &st}); err != nil {
		t.Fatal(err)
	}
	if st.PartsOpened != 3 || st.KeysDecoded == 0 || st.PrefixSkips == 0 {
		t.Fatalf("stats %+v", st)
	}
	if _, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 100}); err != nil {
		t.Fatalf("no stats: %v", err)
	}
}

// TestListRefusalUnchanged: a part missing is the module's refusal, with
// the request's statistics up to it.
func TestListRefusalUnchanged(t *testing.T) {
	dir := listDir(t)
	if err := os.Remove(filepath.Join(dir, "data", "part-00001.parquet")); err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	var st ReadStats
	got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 100, Stats: &st})
	var refusal *RefusalError
	if !errors.As(err, &refusal) || refusal.Code != "missing_part" || got != nil {
		t.Fatalf("list with a part missing: %+v, %v", got, err)
	}
	if st.PartsOpened == 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestListClosedCancelledInvalid(t *testing.T) {
	d, err := OpenDir(context.Background(), listDir(t), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := d.List(ctx, ListRequest{Delimiter: '/', Limit: 1}); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("cancelled: %+v, %v", got, err)
	}
	if got, err := d.List(context.Background(), ListRequest{Delimiter: '/'}); !errors.Is(err, ErrInvalidRequest) || got != nil {
		t.Fatalf("limit 0: %+v, %v", got, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 1}); !errors.Is(err, ErrClosed) || got != nil {
		t.Fatalf("closed: %+v, %v", got, err)
	}
}

// TestEntryCursor: an object's cursor is its row cursor, a prefix's its
// prefix cursor, each owning its bytes.
func TestEntryCursor(t *testing.T) {
	key, prefix := []byte("k"), []byte("p/")
	oc := Entry{Object: &Object{Position: Position{Key: key, Offset: 2}}}.Cursor()
	pc := Entry{CommonPrefix: prefix}.Cursor()
	key[0], prefix[0] = 'x', 'x'
	if !bytes.Equal(oc.Key, []byte("k")) || oc.Offset != 2 || oc.Prefix != nil || !bytes.Equal(pc.Prefix, []byte("p/")) || pc.Key != nil {
		t.Fatalf("cursors %+v %+v", oc, pc)
	}
}

// TestListCursorOutsidePrefix: a cursor before the prefix's keys lists
// from the first of them, one after them lists nothing.
func TestListCursorOutsidePrefix(t *testing.T) {
	d := openDir(t, listDir(t))
	for _, tc := range []struct {
		name   string
		cursor *Cursor
		want   []string
	}{
		{"row before", &Cursor{Key: []byte("a")}, listOracle(listKeys, "c/", '/')},
		{"prefix before", &Cursor{Prefix: []byte("a/")}, listOracle(listKeys, "c/", '/')},
		{"row after", &Cursor{Key: []byte("d")}, nil},
		{"prefix after", &Cursor{Prefix: []byte("c0")}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := listAll(t, d, ListRequest{Prefix: []byte("c/"), Delimiter: '/', Cursor: tc.cursor, Limit: 100})
			if strings.Join(got, " ") != strings.Join(tc.want, " ") {
				t.Fatalf("listed %q, want %q", got, tc.want)
			}
		})
	}
}

// TestListNULDelimiter: Delimiter 0 is the NUL byte, rolled up like any
// other, not a listing without rollup.
func TestListNULDelimiter(t *testing.T) {
	keys := []string{"a", "b\x00x", "b\x00y", "b/c", "c"}
	d := openDir(t, writeDataset(t, s3Spec(), keyRows(keys)))
	got := listAll(t, d, ListRequest{Limit: 2})
	if want := []string{"o:a", "p:b\x00", "o:b/c", "o:c"}; strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("listed %q, want %q", got, want)
	}
}

// TestListReadAhead: a listing with ReadAhead reads key pages ahead
// (ReadStats.ReadAheadReads) and lists exactly what it lists without, in
// one request and paged by its cursors; a Page passes it on, its entries
// and totals the same; none is read ahead by default; a value above
// SeekParquet's most, 64, reads as 64; a negative ReadAhead is
// ErrInvalidRequest before any read.
func TestListReadAhead(t *testing.T) {
	var keys []string
	for p := range 40 {
		for k := range 5 {
			keys = append(keys, fmt.Sprintf("p%03d/%d", p, k))
		}
	}
	dir := writeParts(t, s3Spec(), 2, int64(len(keys)), keyRows(keys))
	want := listOracle(keys, "", '/')
	plain, err := openDir(t, dir).Page(context.Background(), PageRequest{ListRequest: ListRequest{Delimiter: '/', Limit: 1000},
		Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}})
	if err != nil {
		t.Fatal(err)
	}
	for _, ahead := range []int{0, 1, 4, 64, 1000} {
		d := openDir(t, dir)
		var st ReadStats
		got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 1000, Stats: &st, ReadAhead: ahead})
		if err != nil {
			t.Fatal(err)
		}
		listed := make([]string, len(got))
		for i, e := range got {
			listed[i] = describe(e)
		}
		if strings.Join(listed, " ") != strings.Join(want, " ") {
			t.Fatalf("read-ahead %d: listed %q, want %q", ahead, listed, want)
		}
		if (st.ReadAheadReads > 0) != (ahead > 0) || st.PrefixSkips == 0 {
			t.Fatalf("read-ahead %d: stats %+v", ahead, st)
		}
		if paged := listAll(t, openDir(t, dir), ListRequest{Delimiter: '/', Limit: 3, ReadAhead: ahead}); strings.Join(paged, " ") != strings.Join(want, " ") {
			t.Fatalf("read-ahead %d, pages of 3: listed %q, want %q", ahead, paged, want)
		}
		page, err := d.Page(context.Background(), PageRequest{ListRequest: ListRequest{Delimiter: '/', Limit: 1000, Stats: &st, ReadAhead: ahead},
			Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}})
		if err != nil || (st.ReadAheadReads > 0) != (ahead > 0) {
			t.Fatalf("read-ahead %d: page %v, stats %+v", ahead, err, st)
		}
		if !reflect.DeepEqual(page, plain) {
			t.Fatalf("read-ahead %d: page %+v, want %+v", ahead, page, plain)
		}
	}
	d, src := openSource(t, dir)
	src.reset()
	if got, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 10, ReadAhead: -1}); !errors.Is(err, ErrInvalidRequest) || got != nil {
		t.Fatalf("negative read-ahead: %+v, %v", got, err)
	}
	if _, reads := src.taken(); len(reads) != 0 {
		t.Fatalf("a negative read-ahead read %d times", len(reads))
	}
}
