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

// Source is an opened live location: an object store's bucket, opened by
// a provider package such as github.com/varveio/swath/s3. It holds the
// provider's client, credentials and request limiter, which every call
// shares; a call keeps nothing after it returns. It is safe for
// concurrent use. The zero Source is not usable. Package swath names it
// swath.Source.
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

// Lookup says what is at exactly key: present with its entry, absent, or
// unknown with the reason, as package listing states. A key is any
// non-empty object key, "a/" as well as "a": the object at exactly "a/"
// is not the prefix "a/". On S3 a key is absent only when HeadObject finds
// no object and HeadBucket then shows the bucket is there; a 403, a
// missing bucket and a failed probe leave it unknown, with the class of
// the failure as Reason. Each provider package says how its store shows
// a key absent. Lookup returns an error only for a key of the
// wrong form (ErrInvalidRequest) or once ctx has ended.
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
	r.Evidence = evidence(started, finished, listing.StopCompleted)
	return r, nil
}

// Close releases what the source holds open, such as its idle
// connections. The source is not used after Close.
func (s *Source) Close() error {
	return s.store.Close()
}

// evidence is a live answer's: read from the store between started and
// finished, every gap located (Unlocated 0), with stop.
func evidence(started, finished time.Time, stop listing.StopReason) listing.Evidence {
	var unlocated int64
	return listing.Evidence{Origin: listing.OriginLive, Started: &started, Finished: &finished, Unlocated: &unlocated, Stop: stop}
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
