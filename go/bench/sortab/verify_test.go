// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
)

type verified struct {
	rows        int64
	parts       int
	maxPartRows int64
	bytes       int64
}

// verify checks the committed dataset in out against the trace: every key
// of the keyspace exactly once, in order, with its payload (size, time,
// ETag, storage class); each part's rows, bounds and size as the manifest
// says; and strict adjacency, each part's first key after the previous
// part's last.
func verify(out string, l *layout) (verified, error) {
	var v verified
	b, err := os.ReadFile(filepath.Join(out, dataset.ManifestName))
	if err != nil {
		return v, err
	}
	var m dataset.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return v, err
	}
	v.parts = len(m.Parts)
	var next int64
	var prevMax string
	buf := make([]pq.Row, 1024)
	for i, p := range m.Parts {
		v.bytes += p.Bytes
		v.maxPartRows = max(v.maxPartRows, p.Rows)
		if i > 0 && !(string(p.MinKey) > prevMax) {
			return v, fmt.Errorf("part %d starts at %q, not after part %d's last key %q", i, p.MinKey, i-1, prevMax)
		}
		prevMax = string(p.MaxKey)
		f, err := os.Open(filepath.Join(out, p.Path))
		if err != nil {
			return v, err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return v, err
		}
		if fi.Size() != p.Bytes {
			f.Close()
			return v, fmt.Errorf("part %d: %d bytes, manifest %d", i, fi.Size(), p.Bytes)
		}
		pf, err := pq.OpenFile(f, fi.Size())
		if err != nil {
			f.Close()
			return v, err
		}
		if pf.NumRows() != p.Rows {
			f.Close()
			return v, fmt.Errorf("part %d: %d rows, manifest %d", i, pf.NumRows(), p.Rows)
		}
		r := pq.NewReader(pf)
		first := next
		var key []byte
		for {
			n, err := r.ReadRows(buf)
			for _, row := range buf[:n] {
				if next >= l.Keys {
					r.Close()
					f.Close()
					return v, fmt.Errorf("more than %d rows", l.Keys)
				}
				key = l.appendKey(key[:0], next)
				o := l.Object(next)
				if string(row[0].ByteArray()) != string(key) || row[1].Int64() != o.Size || row[2].Int64() != o.Mtime ||
					string(row[3].ByteArray()) != o.ETag || string(row[4].ByteArray()) != o.StorageClass {
					r.Close()
					f.Close()
					return v, fmt.Errorf("row %d: %q size %d mtime %d etag %q class %q; want %q %+v", next, row[0].ByteArray(), row[1].Int64(),
						row[2].Int64(), row[3].ByteArray(), row[4].ByteArray(), key, o)
				}
				next++
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				r.Close()
				f.Close()
				return v, err
			}
		}
		r.Close()
		f.Close()
		if next-first != p.Rows || string(p.MinKey) != l.Key(first) || string(p.MaxKey) != l.Key(next-1) {
			return v, fmt.Errorf("part %d: rows %d..%d, manifest %d rows %q..%q", i, first, next, p.Rows, p.MinKey, p.MaxKey)
		}
	}
	v.rows = next
	if next != l.Keys {
		return v, fmt.Errorf("%d rows of %d", next, l.Keys)
	}
	return v, nil
}
