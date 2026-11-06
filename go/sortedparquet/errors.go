// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import (
	"errors"
	"fmt"
)

// Reasons a set of parts is refused. The first six are the replay server's
// established fallback vocabulary; the others name what only a manifest
// dataset or a missing page index can get wrong.
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
	// whole output (a gap, a duplicate, no final part, or a part that does
	// not match its manifest entry).
	ReasonIncompleteMultifile = "incomplete_multifile"
	// ReasonMixedRowTypes: a row group's row_type statistics do not prove
	// every row is a current object.
	ReasonMixedRowTypes = "mixed_row_types"
	// ReasonSanityFailed: the decoded row-group first keys do not ascend
	// strictly, or a part's decoded bounds contradict its manifest entry.
	ReasonSanityFailed = "sanity_failed"
	// ReasonUnsupportedSchema: the key column is missing or not a required
	// byte array, or a projected column has another type.
	ReasonUnsupportedSchema = "unsupported_schema"
	// ReasonInvalidManifest: a manifest is unreadable or not a sorted
	// dataset manifest this reader knows.
	ReasonInvalidManifest = "invalid_manifest"
	// ReasonNoPageIndex: a non-empty row group's key column lacks a
	// ColumnIndex or an OffsetIndex, so it cannot be seeked by page.
	ReasonNoPageIndex = "no_page_index"
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

// ReasonOf returns err's refusal reason, or "" when err is not a refusal.
func ReasonOf(err error) string {
	var refusal *RefusalError
	if errors.As(err, &refusal) {
		return refusal.Reason
	}
	return ""
}

// ErrCorrupt reports a part whose decoded keys contradict its indexes or the
// order the reader established at Open.
var ErrCorrupt = errors.New("sortedparquet: part contradicts its index")

// ErrClosed reports a read after Close.
var ErrClosed = errors.New("sortedparquet: reader is closed")

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, args...)...)
}
