// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"bytes"
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// Source is an opened live location over a Store. Package source defines
// source.Source on it and documents the methods there.
type Source struct {
	store Store
	// utf8Keys: the store's keys are UTF-8 (NewSource).
	utf8Keys bool
}

// NewSource is the Source over an opened store, which it owns from then
// on: Close closes it. byteKeys says the store's keys are any bytes but
// NUL, as a local directory's names are; otherwise they are UTF-8, as
// every object store's are, and the Source refuses a key or prefix that
// is not UTF-8 before any request, and a listed key that is not as
// listing.ErrInvalidPage.
func NewSource(s Store, byteKeys bool) *Source {
	return &Source{store: s, utf8Keys: !byteKeys}
}

// Lookup answers source.Source.Lookup.
func (s *Source) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	if err := s.checkKey(key); err != nil {
		return listing.LookupResult{}, err
	}

	started := time.Now()
	r, err := s.store.Lookup(ctx, key)
	finished := time.Now()
	if err != nil {
		return listing.LookupResult{}, failure(ctx, err)
	}

	if r.Reason != nil {
		r.Reason = objfile.RedactError(r.Reason)
	}
	r.Evidence = evidence(started, finished, r.State != listing.LookupUnknown)
	return r, nil
}

// Close answers source.Source.Close.
func (s *Source) Close() error {
	return s.store.Close()
}

// evidence is a live answer's: read from the store between started and
// finished. A complete answer has every gap located (Unlocated 0); any
// other has an unknown count (nil).
func evidence(started, finished time.Time, complete bool) listing.Evidence {
	e := listing.Evidence{Started: &started, Finished: &finished}
	if complete {
		var unlocated int64
		e.Unlocated = &unlocated
	}
	return e
}

// failure is a failed call's error: ctx's own once ctx has ended, else
// err, which the store has classed, redacted.
func failure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return objfile.RedactError(err)
}

// invalid is a request this package refuses before any request.
func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", listing.ErrInvalidRequest, fmt.Sprintf(format, args...))
}

// checkKey refuses a key of the wrong form: empty, or as checkName
// refuses it.
func (s *Source) checkKey(key []byte) error {
	if len(key) == 0 {
		return invalid("the key is empty")
	}
	return s.checkName("key", key)
}

// checkName refuses a key or prefix, named what, that holds a NUL byte,
// or that is not UTF-8 when the store's keys are.
func (s *Source) checkName(what string, b []byte) error {
	switch {
	case bytes.IndexByte(b, 0) >= 0:
		return invalid("the %s holds a NUL byte", what)
	case s.utf8Keys && !utf8.Valid(b):
		return invalid("the %s is not UTF-8", what)
	}
	return nil
}
