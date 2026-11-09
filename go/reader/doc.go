// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package reader is what a reader of swath datasets needs to know about
// swath on top of the SeekParquet module (github.com/varveio/seekparquet/go):
// the schema ids and their columns (the descriptors swath's producers write
// from), the manifest's extensions.swath (Metadata), the rows of each
// schema as typed values decoded from a seekparquet.Row, and a Dataset
// that admits and streams a dataset.
//
// Open and OpenDir open a dataset through the module, which reads its
// manifest and no part, parse its metadata and resolve its schema id.
// CheckSchema, at admission, checks the manifest's key against the schema
// id's spec, every part through the module (seekparquet.Dataset.Verify),
// and every part's columns against the spec. AcceptWholeSource accepts the
// dataset as a complete, unfiltered listing of the whole of one source.
// Stream hands on every current object, checking each one's size and
// modification time. None of them checks the parts' digests, which
// seekparquet.Validate does at LevelDigest, or the rows no request decodes.
//
// The module opens, validates and queries the dataset; this package adds
// swath's meaning. It does not import the lister and needs no cloud SDK: a
// reader of a dataset in an object store passes a seekparquet.Source (the
// objfile module's, or its own).
package reader
