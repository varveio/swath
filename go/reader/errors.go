// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"errors"
	"fmt"
	"strconv"

	seekparquet "github.com/varveio/seekparquet/go"
)

// Kind is what a SemanticError found wrong. Its values are stable.
type Kind string

const (
	// KindSchema: a part's columns are not those its schema id defines (a
	// column absent, or of another type), the manifest's key is not the
	// schema id's, the schema id is not one swath writes, or
	// extensions.swath's version is not one this module reads.
	KindSchema Kind = "schema"
	// KindMetadata: the manifest's extensions.swath is malformed (a
	// member of another type than swath writes).
	KindMetadata Kind = "metadata"
	// KindRow: a row holds a value its schema forbids (a null in a required
	// column).
	KindRow Kind = "row"
)

// SemanticError is a dataset that is valid SeekParquet but not what swath's
// schema or metadata says it is. errors.Is matches it against ErrSchema,
// ErrMetadata and ErrRow by Kind.
type SemanticError struct {
	Kind Kind
	// Column names the column at fault (KindSchema, KindRow), "" when
	// none does.
	Column string
	// Row is the row at fault (KindRow, and KindSchema found decoding a
	// row), nil otherwise.
	Row *Position
	Err error
}

// The kinds as targets for errors.Is.
var (
	ErrSchema   = &SemanticError{Kind: KindSchema}
	ErrMetadata = &SemanticError{Kind: KindMetadata}
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
// cause of its own (ErrSchema, ErrMetadata, ErrRow).
func (e *SemanticError) Is(target error) bool {
	t, ok := target.(*SemanticError)
	return ok && t.Err == nil && t.Column == "" && t.Row == nil && t.Kind == e.Kind
}

func semantic(kind Kind, column string, row *Position, format string, args ...any) *SemanticError {
	return &SemanticError{Kind: kind, Column: column, Row: row, Err: fmt.Errorf(format, args...)}
}

// IsRefusal reports whether err, or an error it wraps, refuses the data:
// a *SemanticError or *seekparquet.RefusalError. Unsupported features,
// invalid requests, limits, cancellation and storage errors return false;
// false does not mean retryable. This package does not import objfile:
// its ErrObjectChanged and ErrInvalidSnapshot are the caller's to classify.
func IsRefusal(err error) bool {
	var se *SemanticError
	var re *seekparquet.RefusalError
	return errors.As(err, &se) || errors.As(err, &re)
}
