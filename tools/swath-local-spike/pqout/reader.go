// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package pqout

import (
	"errors"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// Reader streams rows back from a file written by Writer (sort spill runs,
// tests). It uses the generic row API and converts values by column index.
type Reader struct {
	file *os.File
	r    *parquet.Reader
	buf  []parquet.Row
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Reader{file: f, r: parquet.NewReader(pf), buf: make([]parquet.Row, 1024)}, nil
}

// Read fills dst (up to cap) and returns it; len 0 with io.EOF at the end.
func (rd *Reader) Read(dst []record.Row) ([]record.Row, error) {
	dst = dst[:0]
	n, err := rd.r.ReadRows(rd.buf[:min(len(rd.buf), cap(dst))])
	for i := 0; i < n; i++ {
		var r record.Row
		toRecord(&r, rd.buf[i])
		dst = append(dst, r)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return dst, err
	}
	if n == 0 {
		return dst, io.EOF
	}
	return dst, nil
}

func (rd *Reader) Close() error {
	return errors.Join(rd.r.Close(), rd.file.Close())
}

// NumRows in the file.
func (rd *Reader) NumRows() int64 { return rd.r.NumRows() }

func toRecord(r *record.Row, row parquet.Row) {
	for _, v := range row {
		col := v.Column()
		if v.IsNull() {
			continue
		}
		switch col {
		case colKey:
			r.Key = string(v.ByteArray())
		case colKind:
			r.Kind = record.ParseKind(string(v.ByteArray()))
		case colLink:
			r.LinkTarget = string(v.ByteArray())
			r.Present |= record.HasLink
		case colSize:
			r.Size, r.Present = v.Int64(), r.Present|record.HasSize
		case colMtime:
			r.Mtime, r.Present = v.Int64(), r.Present|record.HasMtime
		case colMode:
			r.Mode, r.Present = uint32(v.Int32()), r.Present|record.HasMode
		case colUID:
			r.UID, r.Present = uint32(v.Int32()), r.Present|record.HasUID
		case colGID:
			r.GID, r.Present = uint32(v.Int32()), r.Present|record.HasGID
		case colNlink:
			r.Nlink, r.Present = uint32(v.Int32()), r.Present|record.HasNlink
		case colInode:
			r.Inode, r.Present = v.Int64(), r.Present|record.HasInode
		case colDev:
			r.Dev, r.Present = v.Int64(), r.Present|record.HasDev
		case colBlocks:
			r.Blocks, r.Present = v.Int64(), r.Present|record.HasBlocks
		case colCtime:
			r.Ctime, r.Present = v.Int64(), r.Present|record.HasCtime
		case colAtime:
			r.Atime, r.Present = v.Int64(), r.Present|record.HasAtime
		case colBtime:
			r.Btime, r.Present = v.Int64(), r.Present|record.HasBtime
		}
	}
}
