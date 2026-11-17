// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "errors"

// The error classes. A call that fails returns an error that matches its
// class with errors.Is, and may wrap a cause beside it. A budget stop is
// not an error, and an answer's unknown key is a LookupUnknown with its
// class as the Reason, such as ErrBudgetExceeded for a target a budget
// kept the call from reading. The errors of a cancelled ctx are returned
// as they are, matching no class.
var (
	// ErrInvalidRequest is a request that breaks this contract: a key or
	// prefix of the wrong form, a NUL in a key, a Limit that is not
	// positive, a negative bound. It is refused before any request.
	ErrInvalidRequest = errors.New("listing: invalid request")
	// ErrInvalidToken is a page token issued for another source,
	// inventory, prefix or set of options, or of a version the backend
	// does not read. It is refused before any request.
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
	// source, such as a page of an inventory of versions.
	ErrUnsupported = errors.New("listing: unsupported")
	// ErrInvalidPage is a page from the store that breaks the contract a
	// Page states, or a continuation token from the store that repeats
	// the one sent.
	ErrInvalidPage = errors.New("listing: invalid page from the store")
	// ErrOutOfScope is a question outside an inventory's scope, which the
	// inventory cannot answer: unknown there, never absent.
	ErrOutOfScope = errors.New("listing: outside the inventory's scope")
	// ErrBudgetExceeded is a target that a bound of the request's Budget
	// kept the call from reading: the Reason of a TargetSummary whose
	// State is unknown for that cause, never an error a call returns.
	// Waiting does not change it, so it is not retryable: ask again with
	// a larger budget, or for fewer targets.
	ErrBudgetExceeded = errors.New("listing: a budget kept the call from reading the target")
	// ErrThrottled is a store that kept pushing back past the retry
	// budget.
	ErrThrottled = errors.New("listing: throttled")
	// ErrUnavailable is a store that could not be reached or kept failing
	// past the retry budget: network errors, timeouts, server errors.
	ErrUnavailable = errors.New("listing: unavailable")
)

// lasting are the classes that trying again later does not change.
var lasting = []error{
	ErrInvalidRequest,
	ErrInvalidToken,
	ErrNotFound,
	ErrNotDirectory,
	ErrOtherDevice,
	ErrAuth,
	ErrUnsupported,
	ErrInvalidPage,
	ErrOutOfScope,
	ErrBudgetExceeded,
}

// Retryable reports whether trying the call again later can succeed: err
// matches ErrThrottled or ErrUnavailable and no other class. A nil error,
// an error of no class and a context's error are not retryable.
func Retryable(err error) bool {
	if !errors.Is(err, ErrThrottled) && !errors.Is(err, ErrUnavailable) {
		return false
	}
	for _, class := range lasting {
		if errors.Is(err, class) {
			return false
		}
	}
	return true
}
