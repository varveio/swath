// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import seekparquet "github.com/varveio/seekparquet/go"

// The SeekParquet module's types and errors a reader of swath datasets
// names, as aliases: they are the module's own, so a Source implements
// both, and errors.Is and errors.As match the module's errors either way.
type (
	// Source is a dataset root (seekparquet.Source).
	Source = seekparquet.Source
	// SourceFile is one opened file of a Source.
	SourceFile = seekparquet.SourceFile
	// ContextBinder is a SourceFile whose reads a context can bound.
	ContextBinder = seekparquet.ContextBinder
	// Options configures Open and OpenDir.
	Options = seekparquet.Options
	// Limits bounds what a dataset can make the reader allocate.
	Limits = seekparquet.Limits
	// ReadStats counts one request's reads: diagnostics, not a contract.
	ReadStats = seekparquet.ReadStats
	// Cursor continues a listing after an entry (Entry.Cursor), or a range
	// after a row (Position.Cursor).
	Cursor = seekparquet.Cursor
	// RefusalError is data that breaks the SeekParquet specification; its
	// Code is the contract.
	RefusalError = seekparquet.RefusalError
	// UnsupportedError is a valid dataset or request using a feature the
	// module does not support.
	UnsupportedError = seekparquet.UnsupportedError
)

var (
	// ErrClosed is a read of a closed Dataset.
	ErrClosed = seekparquet.ErrClosed
	// ErrInvalidRequest is a malformed request, such as a limit that is
	// not positive; not a refusal of the data.
	ErrInvalidRequest = seekparquet.ErrInvalidRequest
	// ErrLimitExceeded is input beyond one of the Limits.
	ErrLimitExceeded = seekparquet.ErrLimitExceeded
)
