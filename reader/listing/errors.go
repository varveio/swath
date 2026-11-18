// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "errors"

// The error classes. A call that fails returns an error that matches its
// class with errors.Is, and may wrap a cause beside it. An answer's
// unknown key is a LookupUnknown with its class as the Reason, and a place
// a walk could not read an Unread with its class as the Reason. The errors
// of an ended ctx are returned as they are, matching no class.
var (
	// ErrInvalidRequest is a request that breaks this contract: a key or
	// prefix of the wrong form, a NUL in a key, a Limit that is not
	// positive, a negative number of workers. It is refused before any
	// request. It is also a source configured wrongly for a location that
	// exists, which the store tells: an S3 bucket reached through another
	// region than its own.
	ErrInvalidRequest = errors.New("listing: invalid request")
	// ErrInvalidToken is a page token issued for another source,
	// inventory or prefix, or of a version the backend does not read. It is refused before any request.
	ErrInvalidToken = errors.New("listing: invalid page token")
	// ErrNotFound is a location the call needs that does not exist: the
	// bucket or container, a local root, or a local directory to page or
	// walk. A missing key is LookupAbsent, never this error.
	ErrNotFound = errors.New("listing: not found")
	// ErrNotDirectory is a local prefix that names an entry other than a
	// directory.
	ErrNotDirectory = errors.New("listing: not a directory")
	// ErrOtherDevice is a local prefix on another device than the
	// source's root.
	ErrOtherDevice = errors.New("listing: on another device")
	// ErrAuth is a credential that does not load or that the store
	// refuses, or an access denial.
	ErrAuth = errors.New("listing: not authorized")
	// ErrUnsupported is a question the backend cannot answer for this
	// source, such as a page of an inventory of versions; a source it
	// refuses to open, such as an S3 directory bucket; or a request the
	// store refuses with an answer of no other class, such as an HTTP 400.
	ErrUnsupported = errors.New("listing: unsupported")
	// ErrInvalidPage is a page from the store that breaks the contract a
	// Page states, or a continuation token from the store that repeats
	// the one sent.
	ErrInvalidPage = errors.New("listing: invalid page from the store")
	// ErrOutOfScope is a question outside an inventory's scope, which the
	// inventory cannot answer: unknown there, never absent.
	ErrOutOfScope = errors.New("listing: outside the inventory's scope")
	// ErrUnavailable is a store that could not be reached, kept failing or
	// kept pushing back past the retry budget: network errors, timeouts,
	// server errors, throttling.
	ErrUnavailable = errors.New("listing: unavailable")
)
