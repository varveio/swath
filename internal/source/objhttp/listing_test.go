// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"errors"
	"testing"

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// TestListingClass: each label the command reports a failure by (Label)
// has one listing class, and so do the refusals the command tells apart by
// their marks. A new Class needs its row here.
func TestListingClass(t *testing.T) {
	cause := errors.New("cause")
	failure := func(c Class, status int, transient bool) error {
		return &Failure{Class: c, Status: status, Transient: transient, Cause: cause}
	}
	for _, c := range []struct {
		name string
		err  error
		want error
	}{
		{"auth", failure(ClassAuth, 403, false), listing.ErrAuth},
		{"credentials", &CredentialError{Source: "env", Err: cause}, listing.ErrAuth},
		{"access_denied", failure(ClassAccessDenied, 403, false), listing.ErrAuth},
		{"throttle", &ExhaustedError{Attempts: 9, Err: failure(ClassThrottle, 503, true)}, listing.ErrUnavailable},
		{"timeout", &ExhaustedError{Attempts: 9, Err: failure(ClassTimeout, 0, true)}, listing.ErrUnavailable},
		{"parse", &ExhaustedError{Attempts: 9, Err: failure(ClassParse, 200, true)}, listing.ErrUnavailable},
		{"unsupported", failure(ClassUnsupported, 0, false), listing.ErrUnsupported},
		{"not_found", failure(ClassNotFound, 404, false), listing.ErrNotFound},
		{"region_redirect", failure(ClassRegionRedirect, 301, false), listing.ErrInvalidRequest},
		{"unreachable", failure(ClassUnreachable, 0, false), listing.ErrUnavailable},
		{"server error", &ExhaustedError{Attempts: 9, Err: failure(ClassOther, 500, true)}, listing.ErrUnavailable},
		{"another answer", failure(ClassOther, 400, false), listing.ErrUnsupported},
		{"no answer", failure(ClassOther, 0, false), listing.ErrUnavailable},
		{"a refused source", source.Unsupported(cause), listing.ErrUnsupported},
		{"a protocol violation", source.Violation(cause), listing.ErrInvalidPage},
		{"an invalid page", ErrInvalidPage, listing.ErrInvalidPage},
	} {
		got := Classed(c.err)
		if !errors.Is(got, c.want) || !errors.Is(got, c.err) {
			t.Errorf("%s: %v, want %v around the failure", c.name, got, c.want)
		}
	}
}
