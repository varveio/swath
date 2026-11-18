// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package swath answers the questions of the listing contract
// (github.com/varveio/swath/reader/listing) about a live location: a
// Source is one opened location, read from the store during each call.
//
// The API is experimental: it may change in any release.
//
// A provider package opens a Source: github.com/varveio/swath/s3 an S3
// bucket, github.com/varveio/swath/gcs a Google Cloud Storage bucket,
// github.com/varveio/swath/azblob an Azure Blob Storage container, and
// github.com/varveio/swath/local a directory the caller opened. The
// Source owns the provider's client, credentials and request limiter for
// as long as the caller keeps it, and is safe for concurrent use: every
// call on it shares them, and builds its own state for its own work. Its
// methods answer the four questions, with the request, result, evidence
// and error types of package listing.
//
// Every answer's Evidence has the call's own start and finish: a live
// answer is read during the call, and is not a point-in-time snapshot of a
// location that changes meanwhile. A call that fails returns an error of
// one of package listing's classes, read from the failure as the swath
// command reads it for its outcome, or ctx's own error once ctx has ended.
// Error messages pass through swath's redaction (objfile.RedactError) and
// carry no credential. Every request a call sends, retries included, is
// reported to the objfile.Observer on ctx (objfile.WithObserver): a page,
// and each of an object store walk's, as an objfile.ListRequest, a
// lookup's (S3's HeadObject and HeadBucket, say) as
// objfile.MetadataRequest; a local walk's directory reads are not
// reported. Walk and Summarize set no limit: a caller stops them by
// cancelling ctx.
//
// This package links no cloud SDK; a provider package links its own.
// Neither links swath's command, its flags or its Parquet writer.
package swath
