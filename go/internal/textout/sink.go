// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/varveio/swath/go/internal/schema"
)

// Sink writes the order stage's ascending segments as text rows. It reads
// the payload through the schema's columns by name, so any object-store
// schema with size, last_modified, etag and storage_class columns (S3,
// GCS) can be written; keys must be UTF-8 (schema.String).
type Sink[P any] struct {
	o                         *Out
	size, mtime               func(*P) (int64, bool)
	etag, class               func(*P) (string, bool)
	owner, ownerName, ck, ckT func(*P) (string, bool)
	line                      []byte
	jbuf                      bytes.Buffer
	enc                       *json.Encoder
}

// NewSink binds o to schema s.
func NewSink[P any](o *Out, s *schema.Schema[P]) (*Sink[P], error) {
	if s.Key != schema.String {
		return nil, fmt.Errorf("text output needs an object store source (UTF-8 keys); %s keys are bytes: use a dataset", s.ID)
	}
	k := &Sink[P]{o: o}
	ints := map[string]*func(*P) (int64, bool){"size": &k.size, "last_modified": &k.mtime}
	strs := map[string]*func(*P) (string, bool){"etag": &k.etag, "storage_class": &k.class, "owner_id": &k.owner,
		"owner_display_name": &k.ownerName, "checksum_algorithm": &k.ck, "checksum_type": &k.ckT}
	for _, c := range s.Columns {
		if p, ok := ints[c.Name]; ok && c.Int != nil {
			*p = c.Int
		}
		if p, ok := strs[c.Name]; ok && c.Str != nil {
			*p = c.Str
		}
	}
	if k.size == nil || k.mtime == nil || k.etag == nil || k.class == nil {
		return nil, fmt.Errorf("text output: schema %s lacks the size, last_modified, etag or storage_class column", s.ID)
	}
	k.enc = json.NewEncoder(&k.jbuf)
	k.enc.SetEscapeHTML(false)
	return k, nil
}

func str[P any](f func(*P) (string, bool), p *P) string {
	if f == nil {
		return ""
	}
	v, _ := f(p)
	return v
}

// jsonRow is Java's JSONL object row, in its field order; absent values
// are omitted.
type jsonRow struct {
	Key               string `json:"key"`
	Size              int64  `json:"size"`
	LastModified      string `json:"last_modified,omitempty"`
	ETag              string `json:"etag,omitempty"`
	StorageClass      string `json:"storage_class,omitempty"`
	OwnerID           string `json:"owner_id,omitempty"`
	OwnerDisplayName  string `json:"owner_display_name,omitempty"`
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
	ChecksumType      string `json:"checksum_type,omitempty"`
	RowType           string `json:"row_type"`
}

// rowType is Java's row_type of a listed object (current objects only).
const rowType = "OBJECT"

// Write implements order.Sink.
func (k *Sink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	for _, seg := range segs {
		for i := range seg {
			if err := k.row(&seg[i]); err != nil {
				return err
			}
		}
		k.o.rows += int64(len(seg))
	}
	return k.o.endBatch()
}

func (k *Sink[P]) row(r *schema.Row[P]) error {
	p := &r.Data
	size, _ := k.size(p)
	mt, _ := k.mtime(p)
	b := k.line[:0]
	switch k.o.cfg.Format {
	case TSV:
		b = appendEscaped(b, r.Key)
		b = append(b, '\t')
		b = strconv.AppendInt(b, size, 10)
		b = append(b, '\t')
		b = MtimeText(b, mt)
		b = append(b, '\t')
		b = appendEscaped(b, str(k.etag, p))
		b = append(b, '\t')
		b = appendEscaped(b, str(k.class, p))
		b = append(b, "\t"+rowType+"\n"...)
	case Table:
		// Java's aligned view: size right-aligned in 14, the time
		// left-aligned in 24, then the key; no header.
		var num [20]byte
		n := strconv.AppendInt(num[:0], size, 10)
		for i := len(n); i < 14; i++ {
			b = append(b, ' ')
		}
		b = append(b, n...)
		b = append(b, ' ', ' ')
		at := len(b)
		b = MtimeText(b, mt)
		for i := len(b) - at; i < 24; i++ {
			b = append(b, ' ')
		}
		b = append(b, ' ', ' ')
		b = appendEscaped(b, r.Key)
		b = append(b, '\n')
	case JSONL:
		var t [32]byte
		k.jbuf.Reset()
		if err := k.enc.Encode(jsonRow{Key: r.Key, Size: size, LastModified: string(MtimeText(t[:0], mt)), ETag: str(k.etag, p),
			StorageClass: str(k.class, p), OwnerID: str(k.owner, p), OwnerDisplayName: str(k.ownerName, p),
			ChecksumAlgorithm: str(k.ck, p), ChecksumType: str(k.ckT, p), RowType: rowType}); err != nil {
			return err
		}
		b = append(b, k.jbuf.Bytes()...) // Encode ends the line
	}
	k.line = b
	_, err := k.o.bw.Write(b)
	return err
}
