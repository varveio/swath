// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/reader/listing"
)

// Source is an opened live location, opened by Open or by a provider
// package such as github.com/varveio/swath/source/s3, as the package
// documentation states. The zero Source is not usable.
type Source live.Source

// Page returns the next children of req.Prefix, one level by "/", as
// package listing states: one delimiter listing, the store's retries
// included, and no request per child. Page checks every page before
// returning it as the contract states, and a truncated answer whose
// continuation token is missing is ErrInvalidPage too. A token is bound
// to the source's location: the bucket and what decides which bucket of
// that name it is.
func (s *Source) Page(ctx context.Context, req listing.PageRequest) (listing.Page, error) {
	return (*live.Source)(s).Page(ctx, req)
}

// Lookup says what is at exactly key: present with its entry, absent, or
// unknown with the reason, as package listing states. On S3 a key is
// absent only when HeadObject finds no object and HeadBucket then shows
// the bucket is there; a 403, a missing bucket and a failed probe leave it
// unknown, with the class of the failure as Reason. Each provider package
// says how its store shows a key absent.
func (s *Source) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	return (*live.Source)(s).Lookup(ctx, key)
}

// Walk hands emit every entry at or under req.Prefix, and every place the
// walk could not read and went on past (an Unread), as package listing
// states; each Unread comes in a batch of its own. A zero req.Workers
// selects the source's default, 64 for an object store.
//
// emit is called from the goroutine that called Walk, one batch at a time,
// and nothing is emitted after Walk returns. When ctx ends, Walk returns
// its error, even when emit has accepted every batch. A panic in the
// walk's workers is returned as an error, which need not match a class;
// a panic in emit is not recovered.
func (s *Source) Walk(ctx context.Context, req listing.WalkRequest, emit func(listing.WalkBatch) error) (listing.WalkSummary, error) {
	return (*live.Source)(s).Walk(ctx, req, emit)
}

// Close releases what the source holds open, such as its idle
// connections. The source is not used after Close.
func (s *Source) Close() error {
	return (*live.Source)(s).Close()
}
