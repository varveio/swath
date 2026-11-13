// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package legacy

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
)

// Reasons a set of parts is refused. The first six are the replay server's
// established fallback vocabulary; the others name a schema or a missing
// page index the reader cannot serve.
const (
	// ReasonNoStamp: a part carries no recognised sortedness stamp, or there
	// are no parts.
	ReasonNoStamp = "no_stamp"
	// ReasonUnsupportedMode: a part is stamped with a mode other than
	// current objects (a versions listing).
	ReasonUnsupportedMode = "unsupported_mode"
	// ReasonUnknownFormatVersion: a stamp names a format version this
	// reader does not know.
	ReasonUnknownFormatVersion = "unknown_format_version"
	// ReasonIncompleteMultifile: the parts present do not prove they are the
	// whole output (a gap, a duplicate, no final part).
	ReasonIncompleteMultifile = "incomplete_multifile"
	// ReasonMixedRowTypes: a row group's row_type statistics do not prove
	// every row is a current object.
	ReasonMixedRowTypes = "mixed_row_types"
	// ReasonSanityFailed: the decoded row-group first keys do not ascend
	// strictly, or a row group's first key is empty.
	ReasonSanityFailed = "sanity_failed"
	// ReasonUnsupportedSchema: the key column is missing or not a required
	// byte array, or a projected column has another type.
	ReasonUnsupportedSchema = "unsupported_schema"
	// ReasonNoPageIndex: a non-empty row group's key column lacks a
	// ColumnIndex or an OffsetIndex, so it cannot be seeked by page.
	ReasonNoPageIndex = "no_page_index"
	// ReasonRowGroupDisorder: a read found a row group whose own keys are
	// not in strictly ascending unsigned order (a *DisorderError). Open
	// cannot see it, since it decodes only each group's first key.
	ReasonRowGroupDisorder = "row_group_disorder"
)

// RefusalError reports a set of parts that cannot be served sorted. Reason
// is one of the Reason constants; Detail is for logs.
type RefusalError struct {
	Reason string
	Detail string
}

func (e *RefusalError) Error() string {
	return "sortedparquet: not servable sorted (" + e.Reason + "): " + e.Detail
}

// Refuse returns a *RefusalError with a formatted detail.
func Refuse(reason, format string, args ...any) error {
	return &RefusalError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// ReasonOf returns err's refusal reason: a *RefusalError's Reason,
// ReasonRowGroupDisorder for a *DisorderError, or "" for any other error.
func ReasonOf(err error) string {
	var refusal *RefusalError
	if errors.As(err, &refusal) {
		return refusal.Reason
	}
	var disorder *DisorderError
	if errors.As(err, &disorder) {
		return ReasonRowGroupDisorder
	}
	return ""
}

// DisorderError reports a row group whose keys do not ascend strictly,
// found where its rows are read. It wraps ErrCorrupt; its reason is
// ReasonRowGroupDisorder, the replay server's classification of such a
// fixture, so a caller counts and reports it without matching messages.
type DisorderError struct {
	// Part names the part; RowGroup is the physical row group in it.
	Part     string
	RowGroup int
	// Row is the 0-based row within the row group whose key is at or
	// below its predecessor's, -1 when the disorder is not one row's.
	Row int64
	// Key is that row's key, nil when Row is -1.
	Key []byte
	// Detail says what is out of order, in the replay server's words: for
	// page bounds that do not ascend, "its keys must be in strictly
	// ascending unsigned order, but the column index reports its pages
	// ORDER" (DESCENDING or UNORDERED); for a row, "its keys must be in strictly ascending unsigned order,
	// but row R (HEX) is at or below its predecessor", HEX the key's bytes
	// in lower-case hex (the replay wire contract's fixture_disordered
	// detail; TestDisorderReasonMatchesJava pins it).
	Detail string
}

// pageDisorder returns the *DisorderError of a row group whose key page
// bounds do not ascend: no one row is at fault (Row -1), and the detail
// names the order as the replay server does ("DESCENDING" or
// "UNORDERED").
func pageDisorder(part string, group int, order string) *DisorderError {
	return &DisorderError{Part: part, RowGroup: group, Row: -1,
		Detail: "its keys must be in strictly ascending unsigned order, but the column index reports its pages " + order}
}

// rowDisorder returns the *DisorderError of a row whose key is at or below
// its predecessor's.
func rowDisorder(part string, group int, row int64, key []byte) *DisorderError {
	return &DisorderError{Part: part, RowGroup: group, Row: row, Key: bytes.Clone(key),
		Detail: fmt.Sprintf("its keys must be in strictly ascending unsigned order, but row %d (%s) is at or below its predecessor", row, hex.EncodeToString(key))}
}

func (e *DisorderError) Error() string {
	return fmt.Sprintf("sortedparquet: row group %d of %s is not in strictly ascending unsigned key order: %s", e.RowGroup, e.Part, e.Detail)
}

// Unwrap returns ErrCorrupt.
func (e *DisorderError) Unwrap() error { return ErrCorrupt }

// ErrCorrupt reports a part whose decoded keys contradict its indexes or the
// order the reader established at Open.
var ErrCorrupt = errors.New("sortedparquet: part contradicts its index")

// ErrClosed reports a read after Close.
var ErrClosed = errors.New("sortedparquet: reader is closed")

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, args...)...)
}
