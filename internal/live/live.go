// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package live is the live Source that package
// github.com/varveio/swath/source exposes as source.Source, and what a
// provider package such as github.com/varveio/swath/source/s3 builds one
// over: an opened store (Store), whose Source NewSource makes. It is
// internal, so no code outside swath adds a kind of storage.
package live

import (
	"context"

	"github.com/varveio/swath/reader/listing"
)

// Store is an opened object store: the client, credentials and request
// limiter that every operation on one location shares. It is safe for
// concurrent use. Its errors match a class of package listing, or are
// ctx's own once ctx has ended.
type Store interface {
	// Location identifies what the store reaches: its kind, its bucket or
	// container, and each setting that decides which one of that name it
	// is (an endpoint, a region, an addressing style). Page tokens are
	// bound to it.
	Location() string
	// List makes one delimiter listing: the keys that begin with prefix,
	// those with a "/" after it rolled up into one common prefix each, at
	// most max children, continuing after the store's own token ("" starts
	// the level). The entry at exactly prefix may come beside them, or
	// among them where the store's own page size counts it. It checks what
	// the store answered no further than its client does: the caller
	// checks the level.
	List(ctx context.Context, prefix []byte, token string, max int) (*Level, error)
	// Lookup says what is at exactly key: present, absent, or unknown with
	// the class of the failure that prevented an answer as its Reason. It
	// returns an error only once ctx has ended. The caller sets Evidence.
	Lookup(ctx context.Context, key []byte) (listing.LookupResult, error)
	// Walk lists every entry under w.Prefix into sink, flat, in parallel
	// and in no order, each once: a provider of its own for this call,
	// over the store's client, credentials and limiter, on its range
	// engine, bound to its row schema and counted (Count). It returns when
	// every worker has, with an error that matches a class of package
	// listing, or with ctx's own once ctx has ended. Each place the walk
	// could not read and went on past reaches sink.Unread.
	Walk(ctx context.Context, w Walk, sink Sink) error
	// Close releases what the store holds open.
	Close() error
}

// Level is one answer of a delimiter listing, as the store gave it.
type Level struct {
	// Objects are the entries other than common prefixes, in the store's
	// order: an object store's objects (KindObject); a local store's
	// files, symbolic links and other entries, and its directories as
	// KindDir entries with their metadata, each key a name and a "/".
	Objects []listing.Entry
	// Prefixes are the common prefixes, in the store's order.
	Prefixes [][]byte
	// Truncated says the level goes on after this answer, from Next.
	Truncated bool
	// Next is the store's token for the rest of the level.
	Next string
}
