// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	seekparquet "github.com/varveio/seekparquet/go"
)

// ObjectFields selects optional fields for ScanBatches and, as
// ListRequest.OmitFields, the optional fields a listing leaves out. Key and size are
// always read and checked. Zero selects only key and size.
type ObjectFields uint8

const (
	FieldLastModified ObjectFields = 1 << iota
	FieldETag
	// FieldStorageClass selects access_tier for Azure Blob objects.
	FieldStorageClass
	AllObjectFields = FieldLastModified | FieldETag | FieldStorageClass
)

// BatchScanRequest is a parallel scan with an explicit optional-field selection.
// Its Prefix, Workers and Stats have the same meaning as in ScanRequest.
type BatchScanRequest struct {
	Prefix  []byte
	Workers int
	Stats   *ReadStats
	// Fields selects the optional fields to read and check. Unknown bits
	// are ErrInvalidRequest, before planning. Unselected fields are not read
	// or checked against swath's schema; SeekParquet's structural checks still apply.
	Fields ObjectFields
}

// ObjectBatch is a borrowed, read-only batch of current objects. The batch,
// its keys and byte values are valid only during the ScanBatches callback;
// do not retain them or use them from goroutines that outlive the callback.
// Copy byte values to keep them; Cursor makes an owning copy.
//
// Each batch stays inside a row group, key page, projected page and selected
// span. Batches can be shorter; there is no fixed batch size. All indexed
// accessors panic outside [0, Len()); optional accessors also panic when
// their field was not requested. A false validity result means null.
type ObjectBatch struct {
	raw                         *seekparquet.Batch
	fields                      ObjectFields
	size, modified, etag, class seekparquet.ColumnView
}

// Fields returns the requested optional fields.
func (b *ObjectBatch) Fields() ObjectFields { return b.fields }

// Len returns the number of objects in the batch.
func (b *ObjectBatch) Len() int { return b.raw.Len() }

// Key returns object i's borrowed key bytes.
func (b *ObjectBatch) Key(i int) []byte { return b.raw.Key(i) }

// Size returns object i's size, checked present and non-negative.
func (b *ObjectBatch) Size(i int) int64 { return b.size.Int64(i) }

// LastModified returns UTC microseconds since the Unix epoch and whether
// object i's last_modified is non-null. Requires FieldLastModified.
func (b *ObjectBatch) LastModified(i int) (micros int64, ok bool) {
	b.require(FieldLastModified)
	return b.modified.Int64(i), !b.modified.IsNull(i)
}

// ETag returns object i's borrowed ETag bytes and whether it is non-null.
// Requires FieldETag. Empty and null are distinct.
func (b *ObjectBatch) ETag(i int) ([]byte, bool) {
	b.require(FieldETag)
	return b.etag.Bytes(i), !b.etag.IsNull(i)
}

// StorageClass returns object i's borrowed storage class (access_tier on
// Azure) and whether it is non-null. Requires FieldStorageClass.
func (b *ObjectBatch) StorageClass(i int) ([]byte, bool) {
	b.require(FieldStorageClass)
	return b.class.Bytes(i), !b.class.IsNull(i)
}

// Cursor returns an owning cursor after object i, safe to retain.
func (b *ObjectBatch) Cursor(i int) *Cursor { return b.raw.Cursor(i) }

func (b *ObjectBatch) require(field ObjectFields) {
	if b.fields&field == 0 {
		panic("reader: object field was not requested")
	}
}

// ScanBatches hands fn borrowed column batches of current S3, GCS or Azure
// objects under req.Prefix. Versions and local entries are refused before
// reading, as in Scan. Only size and req.Fields are projected. The manifest's
// key declaration and each selected part's key and projected column types
// are checked against swath's schema, even for all-null columns. These checks
// are stricter than Scan's row decoder (including logical types and key
// uniqueness). Size must be present and non-negative; requested modification times must be in years
// 1..9999. Part schema errors name the part path and have a nil Row; row
// errors own their key and offset and have Scan's semantic kinds.
// Checks run by column before fn; with multiple bad rows the first error may
// differ from Scan's. Unrequested optional fields are neither read nor checked
// against swath's schema. SeekParquet still checks part structure and keys.
//
// Callbacks may run concurrently, in ascending key order within each shard;
// order across shards is unspecified. The first error cancels the other shards.
// Callback and storage errors are returned unchanged (a callback's
// seekparquet.Stop included, as in Scan). Context cancellation,
// statistics (including failure), the key order across shards (a key_order
// refusal returned after every batch was handed on) and StreamStats are as
// in Scan. fn must not
// call Close or wait for a goroutine that does: Close waits for active scans.
// Return an error or cancel the context to stop, then close after this returns.
// Close from another goroutine waits for active callbacks; a later part's
// schema check may return ErrClosed once closing has begun.
func (d *Dataset) ScanBatches(ctx context.Context, req BatchScanRequest, fn func(*ObjectBatch) error) error {
	if req.Stats != nil {
		*req.Stats = ReadStats{}
	}
	if req.Fields & ^AllObjectFields != 0 {
		return fmt.Errorf("%w: unknown object fields %#x", ErrInvalidRequest, req.Fields & ^AllObjectFields)
	}
	all, err := d.currentColumns()
	if err != nil {
		return err
	}
	if err := d.checkKey(); err != nil {
		return err
	}
	columns := []string{all[0]}
	for i, field := range []ObjectFields{FieldLastModified, FieldETag, FieldStorageClass} {
		if req.Fields&field != 0 {
			columns = append(columns, all[i+1])
		}
	}
	return d.scan(ctx, ScanRequest{Prefix: req.Prefix, Workers: req.Workers, Stats: req.Stats}, columns, func() shardScan {
		return func(ctx context.Context, shard *seekparquet.Plan, st *ReadStats, keys *shardKeys) error {
			return d.scanBatchShard(ctx, shard, st, keys, req.Fields, columns, fn)
		}
	})
}

func (d *Dataset) scanBatchShard(ctx context.Context, shard *seekparquet.Plan, st *ReadStats, keys *shardKeys, fields ObjectFields, columns []string, fn func(*ObjectBatch) error) error {
	b := ObjectBatch{fields: fields}
	part := -1
	var failed error // fn's (scanErr)
	err := shard.Scan(ctx, st, func(raw *seekparquet.Batch) error {
		b.raw = raw
		// Release all borrowed references before returning to the decoder,
		// including on errors. The wrapper itself is reused within this shard.
		defer func() { b = ObjectBatch{fields: fields} }()
		if part < 0 || bytes.Compare(raw.Key(0), d.meta.Manifest.Parts[part].MaxKey) > 0 {
			part = sort.Search(len(d.meta.Manifest.Parts), func(i int) bool {
				return bytes.Compare(d.meta.Manifest.Parts[i].MaxKey, raw.Key(0)) >= 0
			})
			if err := d.checkBatchColumns(ctx, part, columns); err != nil {
				return err
			}
		}
		b.size = raw.Column(0)
		c := 1
		for _, slot := range []struct {
			field  ObjectFields
			values *seekparquet.ColumnView
		}{{FieldLastModified, &b.modified}, {FieldETag, &b.etag}, {FieldStorageClass, &b.class}} {
			if fields&slot.field != 0 {
				*slot.values = raw.Column(c)
				c++
			}
		}
		if err := b.check(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		keys.noteBatch(raw)
		failed = fn(&b)
		return failed
	})
	return scanErr(err, failed)
}

// checkBatchColumns checks only selected fields, once per part per shard.
// Planning already checked these parts, so Columns adds no source reads.
func (d *Dataset) checkBatchColumns(ctx context.Context, part int, names []string) error {
	columns, err := d.sp.Columns(ctx, part)
	if err != nil {
		return err
	}
	check := func(spec ColumnSpec) error {
		var col *seekparquet.Column
		for i := range columns {
			if columns[i].Name == spec.Name {
				col = &columns[i]
				break
			}
		}
		if err := checkColumn(col, spec); err != nil {
			return &SemanticError{Kind: KindSchema, Column: spec.Name, Err: fmt.Errorf("part %s: %w", d.meta.Manifest.Parts[part].Path, err)}
		}
		return nil
	}
	if err := check(d.spec.Key); err != nil {
		return err
	}
	for _, name := range names {
		spec, _ := d.spec.Column(name)
		if err := check(spec); err != nil {
			return err
		}
	}
	return nil
}

// check checks every size and requested modification time of the batch.
// The part's columns were checked (checkBatchColumns): size and
// last_modified are present INT64 columns, so their typed accessors apply.
func (b *ObjectBatch) check() error {
	size := b.size
	for i, n := 0, size.Len(); i < n; i++ {
		if err := checkSize(size.Int64(i), !size.IsNull(i), false); err != nil {
			return &SemanticError{Kind: KindRow, Column: "size", Row: b.position(i), Err: err}
		}
	}
	modified := b.modified // not Present, of no rows, when not requested
	for i, n := 0, modified.Len(); i < n; i++ {
		if t := modified.Int64(i); !modified.IsNull(i) && !objectMicrosInRange(t) {
			return semantic(KindRow, "last_modified", b.position(i), "%d µs is outside the years 1 to 9999", t)
		}
	}
	return nil
}

func (b *ObjectBatch) position(i int) *Position {
	return &Position{Key: bytes.Clone(b.Key(i)), Offset: b.raw.Offset(i)}
}
