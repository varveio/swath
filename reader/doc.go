// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package reader supplies swath's metadata, schema registry, schema checks and
// concrete row decoders on top of github.com/varveio/seekparquet/go.
// It depends on no cloud SDK or swath lister package.
//
// Open and OpenDir read the manifest, resolve its registered schema id and parse
// extensions.swath. Dataset.Metadata returns publication, source, scope and
// coverage information; the application decides whether that listing suits its
// purpose. Dataset.CheckSchema checks the manifest's key declaration and every
// part's verified footer and registered columns, without checking all values or
// part digests. Dataset.Close closes the underlying SeekParquet dataset.
//
// Dataset.SeekParquet returns the borrowed dataset for reading: use its Rows,
// List and Plan methods, Plan.ScanParallel, Dataset.Aggregate/AggregateRows and
// Dataset.Page. Name SeekParquet's types and errors directly. The caller chooses
// projections, reductions, filters, budgets, selected-key validation and storage
// cache policy; SeekParquet owns execution and result exactness.
//
// Schema and SchemaIDs expose the producer registry. Project a schema spec's
// ColumnNames to decode owning SeekParquet rows with DecodeS3Object,
// DecodeS3Version, DecodeGCSObject, DecodeGCSGeneration, DecodeAzblobObject,
// DecodeAzblobVersion or DecodeLocalEntry. Nullable fields remain nil; required
// nulls and malformed column values return SemanticError. Application rules such
// as size/time acceptance, filesystem kind filtering and class mapping stay with
// the caller. Position preserves the key and equal-key offset for continuation.
// IsRefusal recognizes swath semantic errors and SeekParquet data refusals.
//
// The swath dataset profile is documented in go/docs/dataset.md in the repository.
package reader
