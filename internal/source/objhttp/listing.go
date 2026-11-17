// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"errors"
	"fmt"

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// ListingClass is the class of the listing contract (reader/listing) that
// a client's failure err falls in. It reads the failure as Label does for
// the command's outcome, so a library call and the command class one
// failure alike:
//
//   - a source refused before listing (source.ErrUnsupported, exit 2 in
//     the command) and a class unsupported are ErrUnsupported;
//   - a broken listing contract (source.ErrProtocol, ErrInvalidPage) is
//     ErrInvalidPage;
//   - auth and access_denied are ErrAuth;
//   - not_found and region_redirect are ErrNotFound: the bucket is not
//     there, or not where the source looks for it;
//   - throttle is ErrThrottled;
//   - an answer outside 2xx that is not transient and has no class of its
//     own, such as a 400, is ErrUnsupported: asking again changes nothing;
//   - anything else, a failure that outlasted the retries (timeout, parse,
//     a server error, a transport failure, unreachable) or one that does
//     not pass (a certificate refused), is ErrUnavailable.
//
// The caller tells its context's own end apart first: it is no class.
func ListingClass(err error) error {
	switch {
	case errors.Is(err, source.ErrUnsupported):
		return listing.ErrUnsupported
	case errors.Is(err, source.ErrProtocol), errors.Is(err, ErrInvalidPage):
		return listing.ErrInvalidPage
	}

	f := Normalize(err)
	switch f.Class {
	case ClassAuth, ClassAccessDenied:
		return listing.ErrAuth
	case ClassThrottle:
		return listing.ErrThrottled
	case ClassNotFound, ClassRegionRedirect:
		return listing.ErrNotFound
	case ClassUnsupported:
		return listing.ErrUnsupported
	}
	if !f.Transient && f.Status >= 400 && f.Status < 500 {
		return listing.ErrUnsupported
	}
	return listing.ErrUnavailable
}

// Classed is err wrapped in its class (ListingClass): errors.Is matches
// both, and its message is the class's, then err's.
func Classed(err error) error {
	return fmt.Errorf("%w: %w", ListingClass(err), err)
}
