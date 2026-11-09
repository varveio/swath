// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	seekparquet "github.com/varveio/seekparquet/go"
)

// CheckSchema checks that the dataset is what its schema id says, for a
// reader admitting it, before any query. The manifest's key column and its
// key uniqueness must be the spec's, so a dataset whose keys repeat (a
// versions listing) never passes as one of current objects, whose keys
// the module then holds strictly ascending. Then the module checks every
// part (seekparquet.Dataset.Verify: its size, footer, stamps, which carry
// the schema id, key column, and the first key page of each row group),
// and CheckSchema reads each part's footer schema: every column of the
// spec is a top-level, non-repeated column of the spec's physical and
// logical type, stored REQUIRED where the spec requires a value, and
// present unless the spec says a part may lack it. Columns the spec does
// not define are ignored.
//
// A disagreement with the spec is a SemanticError of KindSchema naming the
// column and the part; the module's refusals (*seekparquet.RefusalError)
// and storage errors are returned unchanged. It reads one footer per part
// beyond Verify's and no data page: neither the values (Stream checks the
// rows it reads) nor the parts' digests (seekparquet.Validate at
// LevelDigest) are checked.
func (d *Dataset) CheckSchema(ctx context.Context) error {
	key := d.meta.Manifest.Key
	if key.Column != d.spec.Key.Name {
		return semantic(KindSchema, key.Column, nil, "the manifest's key column; %s's is %s", d.spec.ID, d.spec.Key.Name)
	}
	if key.Unique != d.spec.UniqueKeys {
		return semantic(KindSchema, key.Column, nil, "the manifest declares key_unique %t; %s's keys are unique: %t", key.Unique, d.spec.ID, d.spec.UniqueKeys)
	}
	if err := d.sp.Verify(ctx); err != nil {
		return err
	}
	for _, p := range d.meta.Manifest.Parts {
		if err := d.checkPart(ctx, p.Path); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dataset) checkPart(ctx context.Context, path string) error {
	f, err := d.src.Open(ctx, path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var r io.ReaderAt = f
	if b, ok := f.(seekparquet.ContextBinder); ok {
		r = b.WithContext(ctx)
	}
	pf, err := parquet.OpenFile(r, f.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("swath reader: part %s: read the footer: %w", path, err)
	}
	for _, c := range append([]ColumnSpec{d.spec.Key}, d.spec.Columns...) {
		if err := checkColumn(pf.Schema(), c); err != nil {
			return &SemanticError{Kind: KindSchema, Column: c.Name, Err: fmt.Errorf("part %s: %w", path, err)}
		}
	}
	return nil
}

func checkColumn(s *parquet.Schema, c ColumnSpec) error {
	var field parquet.Field
	for _, f := range s.Fields() {
		if f.Name() == c.Name {
			field = f
		}
	}
	switch {
	case field == nil && c.MayBeAbsent:
		return nil
	case field == nil:
		return errors.New("absent from the part")
	case !field.Leaf() || field.Repeated():
		return errors.New("not a top-level, non-repeated column")
	case c.Required && !field.Required():
		return errors.New("optional; the schema requires a value")
	case !typeIs(field.Type(), c.Type):
		return fmt.Errorf("a %s column (%s), not the schema's type", field.Type().Kind(), field.Type())
	}
	return nil
}

// typeIs reports whether a column of type t stores values of the spec's
// type: its physical type, and the logical type that gives the values
// their meaning (a signed integer may be unannotated).
func typeIs(t parquet.Type, want ColumnType) bool {
	var lt format.LogicalTypeValue
	if l := t.LogicalType(); l != nil {
		lt = l.Value
	}
	integer := func(width int8, signed bool) bool {
		i, ok := lt.(*format.IntType)
		return ok && i.BitWidth == width && i.IsSigned == signed || lt == nil && signed
	}
	switch want {
	case TypeString:
		_, ok := lt.(*format.StringType)
		return t.Kind() == parquet.ByteArray && ok
	case TypeBinary:
		return t.Kind() == parquet.ByteArray && lt == nil
	case TypeBool:
		return t.Kind() == parquet.Boolean && lt == nil
	case TypeInt32:
		return t.Kind() == parquet.Int32 && integer(32, true)
	case TypeUint32:
		return t.Kind() == parquet.Int32 && integer(32, false)
	case TypeInt64:
		return t.Kind() == parquet.Int64 && integer(64, true)
	case TypeUint64:
		return t.Kind() == parquet.Int64 && integer(64, false)
	case TypeTimestampMicros:
		ts, ok := lt.(*format.TimestampType)
		if !ok || !ts.IsAdjustedToUTC {
			return false
		}
		_, micros := ts.Unit.Value.(*format.MicroSeconds)
		return t.Kind() == parquet.Int64 && micros
	}
	return false
}
