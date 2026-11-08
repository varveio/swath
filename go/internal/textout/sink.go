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
	// A versions schema (schema.Info.Groups): the S3 version ID or the GCS
	// generation, is_latest or (GCS) time_deleted, is_delete_marker.
	versions            bool
	versionID           func(*P) (string, bool)
	generation, deleted func(*P) (int64, bool)
	latest, marker      func(*P) (bool, bool)
	line                []byte
	pipe                *pipeline[P] // parallel formatters (Config.Formatters > 1), else nil
}

// NewSink binds o to schema s; a TSV output gets its header line here, as
// its columns are the fields s can fill.
func NewSink[P any](o *Out, s *schema.Schema[P]) (*Sink[P], error) {
	if s.Key != schema.String {
		return nil, fmt.Errorf("text output needs an object store source (UTF-8 keys); %s keys are bytes: use a dataset", s.ID)
	}
	k := &Sink[P]{o: o, quoteETag: s.QuotedETag, versions: s.Groups}
	ints := map[string]*func(*P) (int64, bool){"size": &k.size, "last_modified": &k.mtime}
	strs := map[string]*func(*P) (string, bool){"etag": &k.etag, "checksum_algorithm": &k.ckAlgo, "checksum_type": &k.ckType,
		"storage_class": &k.class, "owner_display_name": &k.ownerName, "owner_id": &k.ownerID}
	bools := map[string]*func(*P) (bool, bool){}
	if k.versions {
		// The versions fields, as list-object-versions names them; a GCS
		// generation is its version ID, and the live one is latest.
		strs["version_id"] = &k.versionID
		ints["generation"], ints["time_deleted"] = &k.generation, &k.deleted
		bools["is_latest"], bools["is_delete_marker"] = &k.latest, &k.marker
	}
	for _, c := range s.Columns {
		if p, ok := ints[c.Name]; ok && c.Int != nil {
			*p = c.Int
		}
		if p, ok := strs[c.Name]; ok && c.Str != nil {
			*p = c.Str
		}
		if p, ok := bools[c.Name]; ok && c.Bool != nil {
			*p = c.Bool
		}
	}
	if k.versions && (k.versionID == nil && k.generation == nil || k.latest == nil && k.deleted == nil) {
		return nil, fmt.Errorf("text output: versions schema %s lacks a version ID or latest column", s.ID)
	}
	if k.size == nil || k.mtime == nil {
		return nil, fmt.Errorf("text output: schema %s lacks the size or last_modified column", s.ID)
	}
	if o.cfg.Format == TSV {
		h := []byte("Key")
		if k.versions {
			h = append(h, "\tVersionId\tIsLatest"...)
			if k.marker != nil {
				h = append(h, "\tIsDeleteMarker"...)
			}
		}
		h = append(h, "\tLastModified"...)
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
	if o.cfg.Formatters > 1 {
		k.pipe = startPipeline(k, s)
		o.pipe = k.pipe
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
	t := time.Now()
	defer func() { k.o.sink += time.Since(t) }()
	if k.pipe != nil {
		return k.pipe.send(ctx, segs)
	}
	for _, seg := range segs {
		for i := range seg {
			k.line = k.appendRow(k.line[:0], &seg[i])
			if _, err := k.o.bw.Write(k.line); err != nil {
				return err
			}
		}
		k.o.rows += int64(len(seg))
	}
	return k.o.endBatch()
}

// appendRow appends r's line to b. It reads only what NewSink bound, so
// formatters call it at once.
func (k *Sink[P]) appendRow(b []byte, r *schema.Row[P]) []byte {
	p := &r.Data
	size, hasSize := k.size(p)
	var vid string
	var isLatest, isMarker bool
	if k.versions {
		vid, isLatest, isMarker = k.version(p)
	}
	mt, hasMt := k.mtime(p)
	etag, hasETag := get(k.etag, p)
	if hasETag && k.quoteETag {
		etag = `"` + etag + `"`
	}
	switch k.o.cfg.Format {
	case TSV:
		b = appendEscaped(b, r.Key, false)
		if k.versions {
			b = appendEscaped(append(b, '\t'), vid, false)
			b = strconv.AppendBool(append(b, '\t'), isLatest)
			if k.marker != nil {
				b = strconv.AppendBool(append(b, '\t'), isMarker)
			}
		}
		b = append(b, '\t')
		if hasMt {
			b = appendISO(b, mt)
		}
		b = k.tsv(b, k.etag, etag)
		b = k.tsv(b, k.ckAlgo, val(k.ckAlgo, p))
		b = k.tsv(b, k.ckType, val(k.ckType, p))
		b = append(b, '\t')
		if hasSize {
			b = strconv.AppendInt(b, size, 10)
		}
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
		if !hasSize {
			n = append(num[:0], "-"...) // a delete marker has no size
		}
		for i := len(n); i < 10; i++ {
			b = append(b, ' ')
		}
		b = append(b, n...)
		b = append(b, ' ')
		b = appendEscaped(b, r.Key, false)
		if k.versions {
			b = appendEscaped(append(b, ' '), vid, false)
		}
	case JSONL:
		b = append(b, `{"Key":`...)
		b = appendJSON(b, r.Key)
		if k.versions {
			b = appendJSON(append(b, `,"VersionId":`...), vid)
			b = strconv.AppendBool(append(b, `,"IsLatest":`...), isLatest)
			if k.marker != nil {
				b = strconv.AppendBool(append(b, `,"IsDeleteMarker":`...), isMarker)
			}
		}
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
		if hasSize {
			b = strconv.AppendInt(append(b, `,"Size":`...), size, 10)
		}
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
	return append(b, '\n')
}

// version is a versions row's version ID (S3's, or the GCS generation
// in decimal), whether it is the latest (S3's is_latest; GCS: no
// time_deleted) and whether it is a delete marker.
func (k *Sink[P]) version(p *P) (id string, latest, marker bool) {
	if k.versionID != nil {
		id, _ = k.versionID(p)
	} else if g, ok := k.generation(p); ok {
		id = strconv.FormatInt(g, 10)
	}
	if k.latest != nil {
		latest, _ = k.latest(p)
	} else {
		_, gone := k.deleted(p)
		latest = !gone
	}
	if k.marker != nil {
		marker, _ = k.marker(p)
	}
	return id, latest, marker
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
