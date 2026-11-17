// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package swath answers the questions of the listing contract
// (github.com/varveio/swath/reader/listing) about a live location: a
// Source is one opened location, read from the store during each call.
//
// The API is experimental: it may change in any release.
//
// A provider package opens a Source: github.com/varveio/swath/s3 opens an
// S3 bucket. The Source owns the provider's client, credentials and
// request limiter for as long as the caller keeps it, and is safe for
// concurrent use: every call on it shares them, and builds its own state
// for its own work. It answers Page and Lookup, with the request, result,
// evidence and error types of package listing:
//
//	Page(ctx context.Context, req listing.PageRequest) (listing.Page, error)
//	Lookup(ctx context.Context, key []byte) (listing.LookupResult, error)
//
// Every answer's Evidence has OriginLive and the call's own start and
// finish: a live answer is read during the call, and is not a
// point-in-time snapshot of a location that changes meanwhile. A call that
// fails returns an error of one of package listing's classes, read from
// the failure as the swath command reads it for its outcome, or ctx's own
// error once ctx has ended. Error messages pass through swath's redaction
// (objfile.RedactError) and carry no credential. Every request a call
// sends, retries included, is reported to the objfile.Observer on ctx
// (objfile.WithObserver): a page as an objfile.ListRequest, a lookup's
// HeadObject and HeadBucket as objfile.MetadataRequest.
//
// This package links no cloud SDK; a provider package links its own.
// Neither links swath's command, its flags or its Parquet writer.
package swath
