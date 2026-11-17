// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "time"

// Order names the order of a page's keys. Its values are stable.
type Order string

const (
	// OrderBytes is unsigned byte order of the keys, with the "/" already
	// on directories and prefixes, so a directory "a/" sorts after a file
	// "a.txt". S3, Cloud Storage, local sources and every inventory page
	// in this order.
	OrderBytes Order = "bytes"
	// OrderUTF16 is the order of the keys' UTF-16 code units: a live Azure
	// Blob page, in the service's own order. Sorting each page cannot make
	// byte order across pages.
	OrderUTF16 Order = "utf16"
)

// PageRequest asks for the next children of a prefix: one level, with "/"
// as the only delimiter.
type PageRequest struct {
	// Prefix is the parent: the children are the keys that begin with it.
	// On an object store it is any key prefix; on a local source it is
	// empty, for the root, or a directory's key.
	Prefix []byte
	// Token continues the level after an earlier page: that page's Next,
	// unchanged. Empty starts the level. A token is opaque and versioned:
	// it carries the backend's resume state, the last key returned, and a
	// fingerprint of the source (an inventory's manifest digest), of
	// Prefix and of the options that decide what the level's pages hold:
	// whether ChildTotals is set, and the kinds it counts. A token that
	// does not match them is ErrInvalidToken, refused before any request.
	// Limit and the bounds of work (MaxDuration, the ChildTotals Budget)
	// may change from page to page. Tokens are not signed; a caller that
	// hands them to others signs its own.
	Token string
	// Limit is the most entries the page returns. It must be positive.
	Limit int
	// MaxDuration bounds the time of a page. Reaching it stops the page
	// with StopTimeBudget: it holds no entry, and its Next is the
	// request's Token, which asks again. Zero sets no bound.
	MaxDuration time.Duration
	// ChildTotals, when not nil, asks for the totals of each child prefix
	// or directory, counted as it says and within its Budget. An inventory
	// provides them; a live source returns none.
	ChildTotals *SummaryOptions
}

// Page is one page of a prefix's children. Entries are sorted by Key in
// Order, and every key sorts after the last key of every earlier page of
// the level. A backend checks each page before returning it: every key
// begins with the prefix, names exactly one child, is not empty after the
// prefix, holds no NUL, is valid UTF-8 on an object store, and sorts after
// the key before it, across pages too. A continuation token from the store
// that repeats the one sent is refused. A page that breaks these is
// ErrInvalidPage. Live paging is a traversal, not a snapshot; an
// inventory's pages read one immutable dataset.
type Page struct {
	// Entries are the children, at most the request's Limit. A child
	// object-store prefix is a KindPrefix entry; an object at exactly a
	// child's key comes back inside that child's own level, as its Self.
	Entries []Entry
	// Totals has one element per entry when the request asked for child
	// totals and the backend provides them: a KindPrefix or KindDir
	// entry's totals, as Summarize counts a Subtree target at its key, or
	// nil for any other entry and wherever they are not known within the
	// budget. Nil when not provided.
	Totals []*Totals
	// Self is the entry whose key is exactly the request's Prefix, such as
	// an object "a/" in the level of "a/", on the one page of the level
	// that returns it; it never comes back as a child with an empty name.
	// A nil Self proves nothing about that key: Lookup answers it.
	Self *Entry
	// Order is the order of the keys.
	Order Order
	// Next continues the level: pass it as the next request's Token. A
	// page may be short or empty and still carry one. An empty Next ends
	// the level only when Evidence.Stop is StopCompleted.
	Next string
	// Evidence says where the page came from and whether it completed.
	Evidence Evidence
}
