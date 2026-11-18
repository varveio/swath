// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package swath

import (
	"context"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/reader/listing"
)

// Source is an opened live location, opened by a provider package such as
// github.com/varveio/swath/s3, as the package documentation states. The
// zero Source is not usable.
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
// walk's workers is returned as an error of no class.
func (s *Source) Walk(ctx context.Context, req listing.WalkRequest, emit func(listing.WalkBatch) error) (listing.WalkSummary, error) {
	return (*live.Source)(s).Walk(ctx, req, emit)
}

// Summarize counts the entries of opts.Kinds at or under each target, as
// package listing states. Each target is answered on its own: its State,
// Self and Reason by a Lookup of its key (the root's, the empty key's, is
// unknown: no entry), and a subtree target's entries by one Walk of it.
//
// A subtree whose Lookup left it unknown is still walked, and when
// that walk fails the target counts what it read, incomplete, rather than
// failing the call; so is one shown absent whose walk finds no local
// directory (ErrNotFound), which holds nothing. A walk refused its
// subtree (ErrAuth) or stopped at another device (ErrOtherDevice) counts
// one Unread, as a walk above the target hands it on, and the call goes
// on. A walk that finds no directory to enter (ErrNotDirectory: the key
// names another kind, or is or lies behind a symbolic link, which is
// never followed) makes the target unknown, with that error as its
// Reason, and incomplete, and the call goes on. Any other walk's failure,
// and a lookup's, is Summarize's error;
// when ctx ends, Summarize returns its error.
func (s *Source) Summarize(ctx context.Context, targets []listing.Target, opts listing.SummaryOptions) ([]listing.TargetSummary, listing.Evidence, error) {
	return (*live.Source)(s).Summarize(ctx, targets, opts)
}

// Close releases what the source holds open, such as its idle
// connections. The source is not used after Close.
func (s *Source) Close() error {
	return (*live.Source)(s).Close()
}
