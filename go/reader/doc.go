// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package reader is what a reader of swath datasets needs to know about
// swath on top of the SeekParquet module (github.com/varveio/seekparquet/go):
// the schema ids and their columns (the descriptors swath's producers write
// from), the manifest's extensions.swath (Metadata), the rows of each
// schema as typed values decoded from a seekparquet.Row, a Dataset that
// admits, streams and lists a dataset, and digest validation. The
// module's types and errors a reader names (Source, Options, Cursor,
// RefusalError, ErrClosed, ...) are aliases of the module's own.
//
// Open and OpenDir open a dataset through the module, which reads its
// manifest and no part, parse its metadata (extensions.swath, whose
// version must be MetadataVersion) and resolve its schema id.
// CheckSchema, at admission, checks the manifest's key against the schema
// id's spec, every part through the module (seekparquet.Dataset.Verify),
// and every part's columns against the spec. AcceptWholeSource accepts the
// dataset as a complete, unfiltered listing of the whole of one source.
// Stream hands on every current object, checking each one's size and
// modification time; List lists them by delimiter a page at a time,
// through the module's delimited listing, checking each object as Stream
// does (Delimiter 0 is the NUL byte). None of them checks the parts' digests or the rows no request
// decodes.
//
// ValidateDigests, at import, checks that a dataset is swath's from its
// manifest and that every part declares a SHA-256 its bytes match
// (seekparquet.Validate at LevelDigest); a validation that is incomplete
// is an error, never success. The caller then opens the same bytes.
//
// Totals and Aggregates total the current objects under paths and
// prefixes over the module's selection plans (seekparquet.Dataset.
// PlanRanges): a count is the plan's row count, exact only when every part
// holding the rows has been checked; a byte sum decodes the size pages of
// the rows, once however the selections nest or repeat. Totals is always
// exact or fails. An aggregate is nil when it is not known exactly: a
// count across a part no read has checked, a sum that overflows, or a sum
// left out by its SumMode or by its budget, which counts the compressed
// size pages and dictionaries the sums read, a shared one once, and not
// the footers, key pages and page indexes planning reads.
//
// The module opens, validates and queries the dataset; this package adds
// swath's meaning. It does not import the lister and needs no cloud SDK: a
// reader of a dataset in an object store passes a Source (the
// objfile module's, or its own).
package reader
