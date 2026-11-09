// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"fmt"
	"strconv"
)

// Kind is what a SemanticError found wrong. Its values are stable.
type Kind string

const (
	// KindSchema: a part's columns are not those its schema id defines (a
	// column absent, or of another type), the manifest's key is not the
	// schema id's, the schema id is not one swath writes, it is not one
	// the operation reads (a versions schema has no current objects), or
	// extensions.swath's version is not one this module reads.
	KindSchema Kind = "schema"
	// KindCoverage: the manifest's extensions.swath is malformed, or does
	// not record a listing of the whole source (AcceptWholeSource).
	KindCoverage Kind = "coverage"
	// KindRow: a row holds a value its schema forbids (a null in a required
	// column), or an object a current-object reader refuses (Stream).
	KindRow Kind = "row"
)

// SemanticError is a dataset that is valid SeekParquet but not what swath's
// schema or metadata says it is. errors.Is matches it against ErrSchema,
// ErrCoverage and ErrRow by Kind.
type SemanticError struct {
	Kind Kind
	// Column names the column at fault (KindSchema, KindRow), or the
	// member of extensions.swath at fault (KindCoverage: "source.kind",
	// "coverage.filters"), "" when none does.
	Column string
	// Row is the row at fault (KindRow, and KindSchema found decoding a
	// row), nil otherwise.
	Row *Position
	Err error
}

// The kinds as targets for errors.Is.
var (
	ErrSchema   = &SemanticError{Kind: KindSchema}
	ErrCoverage = &SemanticError{Kind: KindCoverage}
	ErrRow      = &SemanticError{Kind: KindRow}
)

func (e *SemanticError) Error() string {
	msg := "swath reader: " + string(e.Kind)
	if e.Column != "" {
		msg += ": column " + e.Column
	}
	if e.Row != nil {
		msg += ": key " + strconv.Quote(string(e.Row.Key)) + " offset " + strconv.FormatInt(e.Row.Offset, 10)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *SemanticError) Unwrap() error { return e.Err }

// Is reports whether target is a SemanticError of the same kind with no
// cause of its own (ErrSchema, ErrCoverage, ErrRow).
func (e *SemanticError) Is(target error) bool {
	t, ok := target.(*SemanticError)
	return ok && t.Err == nil && t.Column == "" && t.Row == nil && t.Kind == e.Kind
}

func semantic(kind Kind, column string, row *Position, format string, args ...any) *SemanticError {
	return &SemanticError{Kind: kind, Column: column, Row: row, Err: fmt.Errorf(format, args...)}
}
