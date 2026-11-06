// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/varveio/swath/go/internal/schema"
)

// Sink writes the order stage's ascending segments as text rows shaped as
// the S3 API's object (ListObjectsV2 Contents): Key, LastModified, ETag,
// ChecksumAlgorithm, ChecksumType, Size, StorageClass, Owner, in that
// order, so a JSONL line is what `aws s3api list-objects-v2 | jq -c
// '.Contents[]'` prints for the same object. It reads the payload through
// the schema's columns by name (key, last_modified, etag,
// checksum_algorithm, checksum_type, size, storage_class,
// owner_display_name, owner_id); a field whose column the schema lacks is
// left out, and a provider's other columns stay in the dataset. Keys must
// be UTF-8 (schema.String).
type Sink[P any] struct {
	o           *Out
	quoteETag   bool
	size, mtime func(*P) (int64, bool)
	// the string fields, in the API's order around Size
	etag, ckAlgo, ckType, class, ownerName, ownerID func(*P) (string, bool)
	line                                            []byte
}

// NewSink binds o to schema s; a TSV output gets its header line here, as
// its columns are the fields s can fill.
func NewSink[P any](o *Out, s *schema.Schema[P]) (*Sink[P], error) {
	if s.Key != schema.String {
		return nil, fmt.Errorf("text output needs an object store source (UTF-8 keys); %s keys are bytes: use a dataset", s.ID)
	}
	k := &Sink[P]{o: o, quoteETag: s.QuotedETag}
	ints := map[string]*func(*P) (int64, bool){"size": &k.size, "last_modified": &k.mtime}
	strs := map[string]*func(*P) (string, bool){"etag": &k.etag, "checksum_algorithm": &k.ckAlgo, "checksum_type": &k.ckType,
		"storage_class": &k.class, "owner_display_name": &k.ownerName, "owner_id": &k.ownerID}
	for _, c := range s.Columns {
		if p, ok := ints[c.Name]; ok && c.Int != nil {
			*p = c.Int
		}
		if p, ok := strs[c.Name]; ok && c.Str != nil {
			*p = c.Str
		}
	}
	if k.size == nil || k.mtime == nil {
		return nil, fmt.Errorf("text output: schema %s lacks the size or last_modified column", s.ID)
	}
	if o.cfg.Format == TSV {
		h := []byte("Key\tLastModified")
		h = k.header(h, k.etag, "ETag")
		h = k.header(h, k.ckAlgo, "ChecksumAlgorithm")
		h = k.header(h, k.ckType, "ChecksumType")
		h = append(h, "\tSize"...)
		h = k.header(h, k.class, "StorageClass")
		h = k.header(h, k.ownerName, "Owner.DisplayName")
		h = k.header(h, k.ownerID, "Owner.ID")
		if _, err := o.bw.Write(append(h, '\n')); err != nil {
			return nil, err
		}
	}
	return k, nil
}

func (k *Sink[P]) header(h []byte, f func(*P) (string, bool), name string) []byte {
	if f == nil {
		return h
	}
	return append(append(h, '\t'), name...)
}

// Write implements order.Sink.
func (k *Sink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	for _, seg := range segs {
		for i := range seg {
			if _, err := k.o.bw.Write(k.row(&seg[i])); err != nil {
				return err
			}
		}
		k.o.rows += int64(len(seg))
	}
	return k.o.endBatch()
}

func (k *Sink[P]) row(r *schema.Row[P]) []byte {
	p := &r.Data
	size, _ := k.size(p)
	mt, hasMt := k.mtime(p)
	etag, hasETag := get(k.etag, p)
	if hasETag && k.quoteETag {
		etag = `"` + etag + `"`
	}
	b := k.line[:0]
	switch k.o.cfg.Format {
	case TSV:
		b = appendEscaped(b, r.Key, false)
		b = append(b, '\t')
		if hasMt {
			b = appendISO(b, mt)
		}
		b = k.tsv(b, k.etag, etag)
		b = k.tsv(b, k.ckAlgo, val(k.ckAlgo, p))
		b = k.tsv(b, k.ckType, val(k.ckType, p))
		b = append(b, '\t')
		b = strconv.AppendInt(b, size, 10)
		b = k.tsv(b, k.class, val(k.class, p))
		b = k.tsv(b, k.ownerName, val(k.ownerName, p))
		b = k.tsv(b, k.ownerID, val(k.ownerID, p))
	case Table:
		// aws s3 ls: the time to the second, the size right-aligned in
		// 10, the key; in UTC (aws prints local time), so the rows of
		// two runs compare equal wherever they ran.
		if hasMt {
			b = time.UnixMicro(mt).UTC().AppendFormat(b, "2006-01-02 15:04:05")
		} else {
			b = append(b, "                   "...)
		}
		b = append(b, ' ')
		var num [20]byte
		n := strconv.AppendInt(num[:0], size, 10)
		for i := len(n); i < 10; i++ {
			b = append(b, ' ')
		}
		b = append(b, n...)
		b = append(b, ' ')
		b = appendEscaped(b, r.Key, false)
	case JSONL:
		b = append(b, `{"Key":`...)
		b = appendJSON(b, r.Key)
		if hasMt {
			b = append(b, `,"LastModified":"`...)
			b = append(appendISO(b, mt), '"')
		}
		if hasETag {
			b = appendJSON(append(b, `,"ETag":`...), etag)
		}
		if v, ok := get(k.ckAlgo, p); ok {
			// a list in the API; S3 lists one algorithm per object
			b = append(appendJSON(append(b, `,"ChecksumAlgorithm":[`...), v), ']')
		}
		if v, ok := get(k.ckType, p); ok {
			b = appendJSON(append(b, `,"ChecksumType":`...), v)
		}
		b = strconv.AppendInt(append(b, `,"Size":`...), size, 10)
		if v, ok := get(k.class, p); ok {
			b = appendJSON(append(b, `,"StorageClass":`...), v)
		}
		name, hasName := get(k.ownerName, p)
		id, hasID := get(k.ownerID, p)
		if hasName || hasID {
			b = append(b, `,"Owner":{`...)
			if hasName {
				b = appendJSON(append(b, `"DisplayName":`...), name)
			}
			if hasName && hasID {
				b = append(b, ',')
			}
			if hasID {
				b = appendJSON(append(b, `"ID":`...), id)
			}
			b = append(b, '}')
		}
		b = append(b, '}')
	}
	b = append(b, '\n')
	k.line = b
	return b
}

// tsv appends a tab and v when the schema has column f.
func (k *Sink[P]) tsv(b []byte, f func(*P) (string, bool), v string) []byte {
	if f == nil {
		return b
	}
	return appendEscaped(append(b, '\t'), v, false)
}

// get is a column's value; false when the schema lacks it or it is null.
func get[P any](f func(*P) (string, bool), p *P) (string, bool) {
	if f == nil {
		return "", false
	}
	v, ok := f(p)
	return v, ok && v != ""
}

func val[P any](f func(*P) (string, bool), p *P) string {
	v, _ := get(f, p)
	return v
}
