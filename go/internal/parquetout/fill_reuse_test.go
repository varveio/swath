// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"unsafe"
)

// TestFillReuseOutlivesNoPage: two pages whose repeated strings each own
// separately allocated bytes, poisoned as soon as their page's flush
// returns, write the same file as the same rows held in ordinary strings,
// serially and with the columns encoded in parallel. A reused value
// points at an earlier row's bytes; the poison shows nothing reads them
// once their page is written. The rows hold runs of repeats, nulls between
// them, present empty link targets (an accessor's "", true) and repeated
// ETags.
func TestFillReuseOutlivesNoPage(t *testing.T) {
	// rowsAt is rows lo to hi, their strings made by own.
	rowsAt := func(own func(string) string, lo, hi int) []row {
		var rows []row
		for i := lo; i < hi; i++ {
			d := tr{Size: int64(i), Mtime: 1_700_000_000_000_000 + int64(i), Kind: own("FILE")}
			if i%7 == 0 {
				d.Kind = own("DIR")
			}
			switch i % 5 {
			case 0: // null link target
			case 1:
				d.Has |= 2 // present and empty
			default:
				d.Has, d.Link = d.Has|2, own("target/"+strconv.Itoa(i/50))
			}
			if i%3 != 0 {
				d.Has |= 1
			}
			if i%11 != 0 {
				d.ETag = own(fmt.Sprintf("%032x", i/3)) // runs of three; every eleventh null
			}
			rows = append(rows, row{Key: fmt.Sprintf("k%06d", i), Data: d})
		}
		return rows
	}
	dir := t.TempDir()
	want := writeFile(t, filepath.Join(dir, "plain"), rowsAt(func(s string) string { return s }, 0, 2*pageRows), 1)
	for _, columns := range []int{1, 4} {
		var buffers [][]byte // the current page's string bytes
		own := func(s string) string {
			b := []byte(s)
			buffers = append(buffers, b)
			return unsafe.String(unsafe.SliceData(b), len(b))
		}
		var pages [2][]row
		var owned [2][][]byte
		for p := range pages {
			buffers = nil
			pages[p] = rowsAt(own, p*pageRows, (p+1)*pageRows)
			owned[p] = buffers
		}
		c := newCodec(columns)
		f, err := openLocal(nil, filepath.Join(dir, strconv.Itoa(columns)))
		if err != nil {
			t.Fatal(err)
		}
		w, err := create(f, &testSchema, c, func() int { return columns }, "")
		if err != nil {
			t.Fatal(err)
		}
		for p := range pages {
			if err := w.write(pages[p]); err != nil { // a full page: flushed before it returns
				t.Fatal(err)
			}
			for _, b := range owned[p] {
				for i := range b {
					b[i] = '!'
				}
			}
		}
		got, err := w.close()
		c.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got.SHA256 != want.SHA256 {
			t.Fatalf("columns=%d: %s, the plain rows' %s", columns, got.SHA256, want.SHA256)
		}
	}
}
