// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "time"

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
	// fingerprint of the source (an inventory's manifest digest) and of
	// Prefix. A token that does not match them is ErrInvalidToken, refused
	// before any request. Limit and MaxDuration may change from page to
	// page. Tokens are not signed; a caller that hands them to others signs
	// its own.
	Token string
	// Limit is the most children the page returns. It must be positive.
	// The page's Self, the entry at exactly Prefix, is not a child and
	// does not count. A page may hold fewer: a store's own answer may be
	// short, and on an object store the page that holds Self may hold one
	// child fewer, since the store's page size counts it.
	Limit int
	// MaxDuration bounds the time of a page. Reaching it stops the page
	// with StopTimeBudget: it holds no entry, and its Next is the
	// request's Token, which asks again. Zero sets no bound.
	MaxDuration time.Duration
}

// Page is one page of a prefix's children. Entries are sorted by Key in
// unsigned byte order, with the "/" already on directories and prefixes,
// so a directory "a/" sorts after a file "a.txt"; every key sorts after
// the last key of every earlier page of the level. A backend checks each page before returning it: every key
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
	// Self is the entry whose key is exactly the request's Prefix, such as
	// an object "a/" in the level of "a/", on the one page of the level
	// that returns it; it never comes back as a child with an empty name.
	// A nil Self proves nothing about that key: Lookup answers it.
	Self *Entry
	// Next continues the level: pass it as the next request's Token. A
	// page may be short or empty and still carry one. An empty Next ends
	// the level only when Evidence.Stop is StopCompleted or the page holds
	// entries.
	Next string
	// Evidence says where the page came from and whether it completed.
	Evidence Evidence
}
