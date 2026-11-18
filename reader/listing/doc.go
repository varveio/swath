// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package listing is the contract of the questions swath answers about a
// listed location, from a live source or from an opened inventory: their
// requests, results, entries, evidence and error classes. It holds types
// only and imports nothing beyond the standard library, so a program can
// name the contract without linking a lister, a cloud SDK or a Parquet
// reader.
//
// The API is experimental: it may change in any release.
//
// # The questions
//
// Both backends have the first three methods; only an inventory has
// Summarize. The package declares no interface for them: a consumer
// declares the one it needs.
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
// order, and with them, as it meets each one, every place a live walk could
// not read and went on past (an Unread). It lists its scope flat, in
// parallel only when the scope is larger than a page; a page is not a walk
// with parallelism off. It sets no limit: a caller stops a walk by
// cancelling ctx, and Walk then returns the context's error with a
// WalkSummary of what it handed on. A live walk spills nothing to disk and
// retains no key, nor any Unread, after it hands it on.
//
// Summarize counts an inventory's entries at or under each of several
// targets and sums their sizes, space on disk and newest change, one
// TargetSummary per target, in the order of targets, each saying whether
// it is complete. It aggregates key ranges.
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
// A folder's prefix ends in "/". On an object store a prefix is taken
// literally, so a page or walk of "a" holds "ab" and "a.txt" as well as
// what is under "a/"; a local source, and an inventory of one, take "" or
// a directory's key, and refuse a prefix without its "/". On an object
// store a prefix exists if and only if some key begins with it: one with
// none pages and walks empty, never ErrNotFound, and a Lookup of "a/" says
// whether an object has exactly that key, not whether anything is under
// it. A local directory is an entry of its own, present when empty; a
// missing one is ErrNotFound, from an inventory only where it proves the
// absence.
//
// # Evidence
//
// Every answer carries an Evidence: which backend answered, over what
// interval its entries were acquired, the scope it covers, the entries its
// source may lack without recording their keys, and whether the call itself
// completed. Source coverage (Unread, Evidence.Unlocated) and the call's
// completeness (Evidence.Stop) are separate: an exact sum over an
// inventory's rows says nothing about what its listing missed. Nothing is
// proven absent from a source with unlocated gaps, and nothing outside an
// answer's scope is absent: it is unknown.
//
// # Errors
//
// A page that reaches its MaxDuration stops without an error and says so in
// Evidence.Stop; Walk and Summarize have no bound but ctx. A call that
// fails returns an error matching one of the classes ErrInvalidRequest to
// ErrUnavailable with errors.Is. Error messages carry no credential. Every
// storage request a call makes, retries included, is reported to the storage
// observer on ctx (objfile.Observer), as dataset reads are.
//
// # Ownership
//
// A call reads its request and does not keep it. The results of Page, Lookup
// and Summarize belong to the caller. A WalkBatch, and every slice and value
// it reaches, belongs to the backend and is valid only until emit returns.
package listing
