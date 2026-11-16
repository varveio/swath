// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/schema"
)

// TestPageEndsReadBack: the cut file of the parts a router wrote, from the
// page ends its writers collected (Router.Cuts), is the one read back from
// the parts' key page index (KeyPages, PageEnds), as a resume reads its
// retained parts': with keys unique, with version groups across key pages
// (the page end inside a group is not one), and with keys long enough that
// the module ends key pages at its page buffer, inside the writer's. Each
// line's count is the rows of the keys after the previous cut through its
// own.
func TestPageEndsReadBack(t *testing.T) {
	long := testRows(3000, 9)
	for i := range long {
		long[i].Key = strings.Repeat("p", 2000) + long[i].Key
	}
	for name, tc := range map[string]struct {
		rows     []row
		s        *schema.Schema[tr]
		partSize int64
	}{
		"unique":    {testRows(9000, 3), &testSchema, 16 << 10},
		"grouped":   {groupedRows(4000), &groupedSchema, 16 << 10},
		"long keys": {long, &testSchema, 64 << 20},
	} {
		out := t.TempDir()
		if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
			t.Fatal(err)
		}
		r, err := NewRouter(Options{Out: out, PartSize: tc.partSize, Encoders: 2}, tc.s)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < len(tc.rows); i += 500 {
			if err := r.Write(context.Background(), [][]row{tc.rows[i:min(i+500, len(tc.rows))]}); err != nil {
				t.Fatal(err)
			}
		}
		parts, err := r.Close()
		if err != nil {
			t.Fatal(err)
		}
		read := dataset.NewCuts(0)
		pages, inGroup, written := 0, 0, 0
		for _, p := range parts {
			kp, err := KeyPages(p.Path)
			if err != nil {
				t.Fatal(err)
			}
			for i := range kp {
				if i+1 < len(kp) && bytes.Equal(kp[i+1].Min, kp[i].Max) {
					inGroup++
				}
			}
			pages += len(kp)
			written += int(p.Rows+pageRows-1) / pageRows
			ends, err := PageEnds(kp, p.Rows)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			read.Part(ends)
		}
		got, err := r.Cuts().File()
		if err != nil {
			t.Fatal(err)
		}
		want, err := read.File()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: the writers' cut file\n%q\nis not the page index's\n%q (%v)", name, got, want, err)
		}
		switch {
		case len(parts) < 2 && name != "long keys":
			t.Fatalf("%s: %d parts", name, len(parts))
		case name == "grouped" && inGroup == 0:
			t.Fatalf("%s: no key page ends inside a group", name)
		case name == "long keys" && pages <= written:
			t.Fatalf("%s: %d key pages for %d written: the module split none", name, pages, written)
		}
		keys := keysOf(tc.rows)
		at := 0
		for _, line := range strings.Split(strings.TrimSuffix(string(got), "\n"), "\n") {
			i := strings.LastIndexByte(line, '\t')
			key := line[:i]
			n, _ := strconv.Atoi(line[i+1:])
			from := at
			for at < len(keys) && keys[at] <= key {
				at++
			}
			if at-from != n || keys[at-1] != key {
				t.Fatalf("%s: line %.40q…: %d rows, %d keys through it", name, line, n, at-from)
			}
		}
		if at != len(keys) {
			t.Fatalf("%s: the cut file covers %d of %d rows", name, at, len(keys))
		}
	}
}

// TestPageCutAtKeyBytes: a page ends at pageKeyBytes of keys before its
// pageRows rows, so keys past the price (unboundedKeyBytes: a local path
// deeper than PATH_MAX) keep a page within what the writer is admitted
// at; keys within the price fill whole pages.
func TestPageCutAtKeyBytes(t *testing.T) {
	for _, key := range []int{unboundedKeyBytes, 2 * unboundedKeyBytes} {
		rows := testRows(3*pageRows, 1)
		for i := range rows {
			rows[i].Key = strings.Repeat("p", key-8) + fmt.Sprintf("%08d", i)
		}
		f, err := openLocal(nil, filepath.Join(t.TempDir(), "part"))
		if err != nil {
			t.Fatal(err)
		}
		c := newCodec(1)
		w, err := create(f, &testSchema, c, func() int { return 1 }, "")
		if err != nil {
			t.Fatal(err)
		}
		if err := w.write(rows); err != nil {
			t.Fatal(err)
		}
		per := int64(min(pageRows, pageKeyBytes/key))
		for i, e := range w.ends {
			if e.Rows != int64(i+1)*per || e.Key != rows[e.Rows-1].Key {
				t.Fatalf("keys of %d bytes: page end %d at %d rows, want %d", key, i, e.Rows, int64(i+1)*per)
			}
		}
		if p, err := w.close(); err != nil || p.Rows != int64(len(rows)) {
			t.Fatalf("close: %v", err)
		}
		c.Close()
	}
}
