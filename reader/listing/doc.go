// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package listing is the contract of the four questions swath answers about
// a listed location, from a live source or from an opened inventory: their
// requests, results, entries, evidence and error classes. It holds types
// only and imports nothing beyond the standard library, so a program can
// name the contract without linking a lister, a cloud SDK or a Parquet
// reader.
//
// The API is experimental: it may change in any release.
//
// # The four questions
//
// Both backends have this method set. The package declares no interface for
// it: a consumer declares the one it needs.
//
//	Page(ctx context.Context, req PageRequest) (Page, error)
//	Lookup(ctx context.Context, key []byte) (LookupResult, error)
//	Walk(ctx context.Context, req WalkRequest, emit func(WalkBatch) error) (WalkSummary, error)
//	Summarize(ctx context.Context, targets []Target, opts SummaryOptions) ([]TargetSummary, Evidence, error)
//
// Page returns the next children of a prefix, one level deep, sorted and
// resumable by token. A live page is one delimiter listing; an inventory's is
// a seek in its sorted dataset.
//
// Lookup says what is at exactly one key: present with its Entry, absent, or
// unknown with the reason. Absence is never a fallback. It returns an error
// only when it gives no answer: a request it refuses, or ctx ending.
//
// Walk hands every entry at or under a prefix to emit, exactly once, in no
// order; a live walk adds every extent it did not read as an Unread. It lists
// its scope flat, in parallel only when the scope is larger than a page; a
// page is not a walk with parallelism off. A live walk spills nothing to disk
// and retains no key after it returns. When ctx ends, Walk returns the
// context's error.
//
// Summarize counts the entries at or under each of several targets and sums
// their sizes, space on disk and newest change, one TargetSummary per target,
// in the order of targets. A live source walks the union of the targets once;
// an inventory aggregates key ranges.
//
// # Keys
//
// A key is bytes relative to the resource: to an object store's bucket or
// container, or to a local source's root. The keys of directories and of
// object-store common prefixes end in "/", so "a" and "a/" are different
// keys. No key holds a NUL byte. Where this package states an order, it is
// unsigned byte order of the keys (bytes.Compare), unless a Page names
// another.
//
// # Evidence
//
// Every answer carries an Evidence: which backend answered, over what
// interval its entries were acquired, the scope it covers, the entries its
// source did not read, and whether the call itself completed. Source coverage
// (Unread, Evidence.Unlocated) and the call's completeness (Evidence.Stop)
// are separate: an exact sum over an inventory's rows says nothing about what
// its listing missed. Nothing is proven absent from a source with unlocated
// gaps, and nothing outside an answer's scope is absent: it is unknown.
//
// # Budgets and errors
//
// A call that reaches a bound of its request stops without an error and says
// which in Evidence.Stop. A call that fails returns an error matching one of
// the classes ErrInvalidRequest to ErrUnavailable with errors.Is, and
// Retryable says whether trying it again later can succeed. Error messages
// carry no credential. Every storage request a call makes, retries included,
// is reported to the storage observer on ctx (objfile.Observer), as dataset
// reads are.
//
// # Ownership
//
// A call reads its request and does not keep it. The results of Page, Lookup
// and Summarize belong to the caller. A WalkBatch, and every slice and value
// it reaches, belongs to the backend and is valid only until emit returns.
package listing
