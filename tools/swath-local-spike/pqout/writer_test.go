// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package pqout

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

func testRows(n int) []record.Row {
	rows := make([]record.Row, n)
	for i := range rows {
		r := record.Row{Key: fmt.Sprintf("dir%03d/file-%07d.dat", i/1000, i), Kind: record.KindFile, Size: int64(i * 37), Mtime: 1_700_000_000_000_000 + int64(i)}
		r.Present = record.HasSize | record.HasMtime
		if i%3 == 0 {
			r.Present |= record.HasStat
			r.Mode, r.UID, r.GID, r.Nlink, r.Inode, r.Dev, r.Blocks = 0o644, 1000, 1000, 1, int64(i), 2049, 4096
			r.Ctime, r.Atime = r.Mtime+1, r.Mtime+2
		}
		if i%101 == 0 {
			r.Kind, r.LinkTarget, r.Present = record.KindSymlink, "../t", r.Present|record.HasLink
		}
		rows[i] = r
	}
	return rows
}

func writeAll(t *testing.T, path string, rows []record.Row, encoders int) PartInfo {
	cfg := DefaultConfig()
	cfg.GroupBytes = 256 << 10
	cfg.Encoders = encoders
	w, err := Create(path, cfg, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	info, err := w.Close(true)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// Column-parallel page encoding must produce exactly the serial bytes.
func TestParallelEncodingByteIdentical(t *testing.T) {
	rows := testRows(150_000)
	dir := t.TempDir()
	serial := writeAll(t, filepath.Join(dir, "serial.parquet"), rows, 1)
	if serial.Groups < 2 {
		t.Fatalf("want several row groups, got %d", serial.Groups)
	}
	for _, enc := range []int{2, 4, 15} {
		par := writeAll(t, filepath.Join(dir, fmt.Sprintf("par%d.parquet", enc)), rows, enc)
		if par.MD5 != serial.MD5 || par.Bytes != serial.Bytes || par.Groups != serial.Groups {
			t.Fatalf("encoders=%d: %s/%d/%d differs from serial %s/%d/%d", enc, par.MD5, par.Bytes, par.Groups, serial.MD5, serial.Bytes, serial.Groups)
		}
	}
	rd, err := Open(filepath.Join(dir, "par4.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	var got []record.Row
	buf := make([]record.Row, 0, 777)
	for {
		buf, err = rd.Read(buf)
		got = append(got, buf...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != len(rows) {
		t.Fatalf("rows %d", len(got))
	}
	for i := range rows {
		if got[i] != rows[i] {
			t.Fatalf("row %d: %+v != %+v", i, got[i], rows[i])
		}
	}
}

func TestRejectsUnsortedKeys(t *testing.T) {
	w, err := Create(filepath.Join(t.TempDir(), "x.parquet"), DefaultConfig(), false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	a, b := record.Row{Key: "b"}, record.Row{Key: "a"}
	if err := w.Write(&a); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(&b); err == nil {
		t.Fatal("expected ordering error")
	}
}
