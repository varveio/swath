// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"

	seekparquet "github.com/varveio/seekparquet/go"
)

// CheckSchema checks that the dataset is what its schema id says, for a
// caller that wants every part checked before a query. The manifest's key column and its
// key uniqueness must be the spec's, so a dataset whose keys repeat (a
// versions listing) never passes as one of current objects, whose keys
// the module then holds strictly ascending. Then the module checks every
// part (seekparquet.Dataset.Verify: its size, footer, stamps, which carry
// the schema id, key column, and the first key page of each row group),
// and CheckSchema reads each part's columns from the footer the module
// checked and reads the part through (seekparquet.Dataset.Columns), so
// what it checks is what later reads decode: every column of the
// spec is a top-level, non-repeated column of the spec's physical and
// logical type, stored REQUIRED where the spec requires a value, and
// present unless the spec says a part may lack it. Columns the spec does
// not define are ignored.
//
// A disagreement with the spec is a SemanticError of KindSchema naming the
// column and the part; the module's refusals (*seekparquet.RefusalError)
// and storage errors are returned unchanged. It reads nothing beyond
// Verify's reads: neither the values nor the parts' digests are checked.
func (d *Dataset) CheckSchema(ctx context.Context) error {
	if err := d.checkKey(); err != nil {
		return err
	}
	if err := d.sp.Verify(ctx, nil); err != nil {
		return err
	}
	for i, p := range d.meta.Manifest.Parts {
		if err := d.checkPart(ctx, i, p.Path); err != nil {
			return err
		}
	}
	return nil
}

// checkKey checks the manifest's key column and key uniqueness against
// the spec's.
func (d *Dataset) checkKey() error {
	key := d.meta.Manifest.Key
	if key.Column != d.spec.Key.Name {
		return semantic(KindSchema, key.Column, nil, "the manifest's key column; %s's is %s", d.spec.ID, d.spec.Key.Name)
	}
	if key.Unique != d.spec.UniqueKeys {
		return semantic(KindSchema, key.Column, nil, "the manifest declares key_unique %t; %s's keys are unique: %t", key.Unique, d.spec.ID, d.spec.UniqueKeys)
	}
	return nil
}

// checkPart checks part i's columns, from the footer the module checked
// and reads the part through.
func (d *Dataset) checkPart(ctx context.Context, i int, path string) error {
	columns, err := d.sp.Columns(ctx, i)
	if err != nil {
		return err
	}
	byName := map[string]*seekparquet.Column{}
	for j := range columns {
		byName[columns[j].Name] = &columns[j]
	}
	for _, c := range append([]ColumnSpec{d.spec.Key}, d.spec.Columns...) {
		if err := checkColumn(byName[c.Name], c); err != nil {
			return &SemanticError{Kind: KindSchema, Column: c.Name, Err: fmt.Errorf("part %s: %w", path, err)}
		}
	}
	return nil
}

// checkColumn checks a part's top-level column against its spec; col is
// nil when the part has no column of the spec's name.
func checkColumn(col *seekparquet.Column, c ColumnSpec) error {
	switch {
	case col == nil && c.MayBeAbsent:
		return nil
	case col == nil:
		return errors.New("absent from the part")
	case col.Group || col.Repetition == seekparquet.Repeated:
		return errors.New("not a top-level, non-repeated column")
	case c.Required && col.Repetition != seekparquet.Required:
		return errors.New("optional; the schema requires a value")
	case !typeIs(col, c.Type):
		return fmt.Errorf("a %s column (%s), not the schema's type", col.Type, col.Logical)
	}
	return nil
}

// typeIs reports whether a column stores values of the spec's type: its
// physical type, and the logical type that gives the values their meaning
// (a signed integer may be unannotated).
func typeIs(col *seekparquet.Column, want ColumnType) bool {
	lt := col.Logical
	none := lt.Kind == seekparquet.LogicalNone
	integer := func(width int8, signed bool) bool {
		return lt.Kind == seekparquet.LogicalInt && lt.BitWidth == width && lt.Signed == signed || none && signed
	}
	switch want {
	case TypeString:
		return col.Type == seekparquet.KindByteArray && lt.Kind == seekparquet.LogicalString
	case TypeBinary:
		return col.Type == seekparquet.KindByteArray && none
	case TypeBool:
		return col.Type == seekparquet.KindBoolean && none
	case TypeInt32:
		return col.Type == seekparquet.KindInt32 && integer(32, true)
	case TypeUint32:
		return col.Type == seekparquet.KindInt32 && integer(32, false)
	case TypeInt64:
		return col.Type == seekparquet.KindInt64 && integer(64, true)
	case TypeUint64:
		return col.Type == seekparquet.KindInt64 && integer(64, false)
	case TypeTimestampMicros:
		return col.Type == seekparquet.KindInt64 && lt.Kind == seekparquet.LogicalTimestamp &&
			lt.AdjustedToUTC && lt.Unit == seekparquet.Micros
	}
	return false
}
