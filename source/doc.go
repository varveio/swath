// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package source is swath's live listing library. A Source is an opened
// live location: it answers the questions of the listing contract
// (github.com/varveio/swath/reader/listing) about that location, read from
// the store during each call.
//
// The API is experimental: it may change in any release.
//
// Four constructors open a Source, each in its own package under this one:
// s3.Open (github.com/varveio/swath/source/s3) an S3 bucket, gcs.Open
// (github.com/varveio/swath/source/gcs) a Google Cloud Storage bucket,
// azblob.Open (github.com/varveio/swath/source/azblob) an Azure Blob
// Storage container, and local.Open (github.com/varveio/swath/source/local)
// a directory the caller opened. The Source owns the provider's client,
// credentials and request limiter for as long as the caller keeps it, and
// is safe for concurrent use: every call on it shares them, and builds its
// own state for its own work. Its methods Page, Lookup and Walk answer with
// the request, result, evidence and error types of package listing
// (github.com/varveio/swath/reader/listing). A swath inventory, a dataset
// already written, is read through the same contract by package view
// (github.com/varveio/swath/reader/view).
//
// Every answer's Evidence has the call's own start and finish: a live
// answer is read during the call, and is not a point-in-time snapshot of a
// location that changes meanwhile. A call that fails returns an error of
// one of package listing's classes, read from the failure as the swath
// command reads it for its outcome, or ctx's own error once ctx has ended.
// A walk has two exceptions: an error returned by the caller's own emit,
// which Walk returns unchanged, and a panic recovered inside the walk,
// which Walk returns as an error of no class.
// Error messages pass through swath's redaction (objfile.RedactError) and
// carry no credential. Every request a call sends, retries included, is
// reported to the objfile.Observer on ctx (objfile.WithObserver): a page,
// and each of an object store walk's, as an objfile.ListRequest, a
// lookup's (S3's HeadObject and HeadBucket, say) as
// objfile.MetadataRequest; a local walk's directory reads are not
// reported. Walk sets no limit: a caller stops it by cancelling ctx.
//
// This package links no cloud SDK; a provider package links its own.
// Neither links swath's command, its flags or its Parquet writer.
package source
