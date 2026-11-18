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

// Source is an opened live location over a Store. Package swath defines
// swath.Source on it and documents the methods there.
type Source struct {
	store Store
	// utf8Keys: the store's keys are UTF-8, unless it is a ByteKeys.
	utf8Keys bool
}

// NewSource is the Source over an opened store, which it owns from then
// on: Close closes it.
func NewSource(s Store) *Source {
	_, raw := s.(ByteKeys)
	return &Source{store: s, utf8Keys: !raw}
}

// Lookup answers swath.Source.Lookup.
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
	stop := listing.StopCompleted
	if r.State == listing.LookupUnknown {
		stop = "" // not answered: nothing reads as complete
	}
	r.Evidence = evidence(started, finished, stop)
	return r, nil
}

// Close answers swath.Source.Close.
func (s *Source) Close() error {
	return s.store.Close()
}

// evidence is a live answer's: read from the store between started and
// finished, with stop. A completed answer has every gap located
// (Unlocated 0); any other has an unknown count (nil).
func evidence(started, finished time.Time, stop listing.StopReason) listing.Evidence {
	e := listing.Evidence{Started: &started, Finished: &finished, Stop: stop}
	if stop == listing.StopCompleted {
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
