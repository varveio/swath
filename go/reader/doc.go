// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package reader is what a reader of swath datasets needs to know about
// swath on top of the SeekParquet module (github.com/varveio/seekparquet/go):
// the schema ids and their columns (the descriptors swath's producers write
// from), the manifest's extensions.swath (Metadata), the rows of each
// schema as typed values decoded from a seekparquet.Row, and a Dataset that
// opens, checks, streams and lists a dataset. The module's types and errors
// a reader names (Source, Options, Cursor, RefusalError, ErrClosed, ...)
// are aliases of the module's own.
//
// Open and OpenDir open a dataset through the module, which reads its
// manifest and no part, parse its metadata (extensions.swath, whose
// version must be MetadataVersion) and resolve its schema id. Metadata
// returns what the manifest records: the source, the scope, the coverage
// (filters such as one_filesystem included) and the publication time. The
// reader opens and reads; it does not judge a dataset. Whether a partial,
// filtered or unpublished listing will do, whether it is of the source an
// application expects, and whether a reopened dataset is the one it saw
// before are the application's decisions.
//
// CheckSchema checks the manifest's key against the schema id's spec,
// every part through the module (seekparquet.Dataset.Verify), and every
// part's columns against the spec. Stream hands on every current object,
// checking each one's size and modification time; StreamLocal hands on a
// filesystem's entries (LocalEntry), nulls kept; Scan hands on the current
// objects under a prefix in batches, from shards of the plan read
// concurrently (seekparquet.Plan.Split); its statistics include those of
// a plan or split that failed. List lists
// objects or entries by delimiter a page at a time, through the module's
// delimited listing, checking each as Stream does (Delimiter 0 is the NUL
// byte); a common prefix carries the row at its exact key, an S3-style
// folder object or a filesystem's directory row, from the same request.
// Page lists a page with one entry of lookahead for its Next cursor and
// the totals (Aggregates) of the prefixes it returns. None of them
// checks the parts' digests or the rows no request decodes: part digests
// are checked when a dataset is copied or moved, not when it is read.
// IsRefusal tells the errors that refuse the data from the others.
//
// Totals and Aggregates total the current objects, or a filesystem's
// entries of every kind and their st_size (entry bytes), under paths and
// prefixes over the module's selection plans (seekparquet.Dataset.Plan):
// an aggregate count is the plan's row count, exact only when
// every part holding the rows has been checked, and a Totals count the rows
// its scan reads; a byte sum decodes the size pages of the rows, once
// however the selections nest or repeat. Totals scans in parallel shards,
// as Scan does; Aggregates in one. Totals is always exact or fails, its
// byte total nil over an entry without a size. An aggregate is nil when
// it is not known exactly: a count across a part no read has checked, a
// sum over an entry without a size, a sum that overflows, or a sum left
// out by its SumMode or by its budget, which counts the compressed size
// pages and dictionaries the sums read, a shared one once, and not the
// footers, key pages and page indexes planning reads.
//
// The module opens, validates and queries the dataset; this package adds
// swath's meaning. It does not import the lister and needs no cloud SDK: a
// reader of a dataset in an object store passes a Source (the
// objfile module's, or its own).
package reader
