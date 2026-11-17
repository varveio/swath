// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// Page returns the next children of req.Prefix, one level by "/", in byte
// order (OrderBytes): one delimiter listing, the store's retries included,
// and no request per child. The prefix is any key prefix, taken
// literally: "a" holds both "ab" and the prefix "a/". A child prefix is a
// KindPrefix entry; the object at exactly the prefix comes back once, as
// the Self of the page that reads it, never as a child. req.Limit bounds
// the children; Self comes beside them. A store's listing of req.Limit
// entries may count Self among them, so the page that holds Self may hold
// one child fewer: a token bound to its request's page size cannot be
// continued with another.
//
// Page checks every page before returning it, as package listing states:
// a key outside the prefix, below one of its children, holding a NUL, not
// UTF-8, or not after the key before it, across pages too, more children
// than asked for, and a truncated answer whose continuation token is
// missing or repeats the one sent are ErrInvalidPage.
//
// A token is opaque and versioned. It carries the store's continuation
// token and the level's last key, and is bound to the source's location
// (the bucket and what decides which bucket of that name it is) and to
// the prefix; one issued for another is ErrInvalidToken, before any
// request. A live source gives no child totals: req.ChildTotals is
// ErrUnsupported, before any request. When req.MaxDuration ends a page it
// holds no entry, its Next is req.Token, which asks again, and its
// Evidence.Stop is StopTimeBudget.
func (s *Source) Page(ctx context.Context, req listing.PageRequest) (listing.Page, error) {
	if err := s.checkPage(req); err != nil {
		return listing.Page{}, err
	}
	bind := binding(s.store.Location(), req.Prefix)
	cur, err := decodeToken(req.Prefix, bind, req.Token)
	if err != nil {
		return listing.Page{}, err
	}

	op, cancel := withBudget(ctx, req.MaxDuration)
	defer cancel()
	started := time.Now()
	lvl, err := s.store.List(op, req.Prefix, cur.store, req.Limit)
	finished := time.Now()
	if err != nil {
		if timeUp(ctx, op) {
			return listing.Page{Order: listing.OrderBytes, Next: req.Token, Evidence: evidence(started, finished, listing.StopTimeBudget)}, nil
		}
		return listing.Page{}, failure(ctx, err)
	}

	page, last, err := s.level(req.Prefix, cur.last, lvl, req.Limit)
	if err != nil {
		return listing.Page{}, objfile.RedactError(err) // it quotes the store's keys
	}
	if lvl.Truncated {
		if err := checkContinuation(cur.store, lvl.Next); err != nil {
			return listing.Page{}, err
		}
		page.Next = encodeToken(bind, cursor{last: last, store: lvl.Next})
	}
	page.Evidence = evidence(started, finished, listing.StopCompleted)
	return page, nil
}

// checkPage refuses a request of the wrong form, or one asking for child
// totals, before any request.
func (s *Source) checkPage(req listing.PageRequest) error {
	if err := s.checkName("prefix", req.Prefix); err != nil {
		return err
	}
	switch {
	case req.Limit < 1:
		return invalid("a page's Limit is positive")
	case req.MaxDuration < 0:
		return invalid("a negative bound")
	case req.ChildTotals != nil:
		return fmt.Errorf("%w: a live source gives no child totals", listing.ErrUnsupported)
	}
	return nil
}

// level checks one answer of the store against the level of prefix, after
// the level's last key so far (nil before any), and makes it a page of at
// most limit children: the entries and common prefixes merged in byte
// order, and the entry at exactly prefix as Self, which a store may add
// beyond them. It returns the page and the level's last key after it.
func (s *Source) level(prefix, last []byte, lvl *Level, limit int) (listing.Page, []byte, error) {
	page := listing.Page{Order: listing.OrderBytes}
	objects, prefixes := lvl.Objects, lvl.Prefixes
	page.Entries = make([]listing.Entry, 0, len(objects)+len(prefixes))
	for len(objects) > 0 || len(prefixes) > 0 {
		var e listing.Entry
		if len(prefixes) == 0 || len(objects) > 0 && bytes.Compare(objects[0].Key, prefixes[0]) < 0 {
			e, objects = objects[0], objects[1:]
		} else {
			e, prefixes = listing.Entry{Key: prefixes[0], Kind: listing.KindPrefix}, prefixes[1:]
		}
		if err := s.checkEntry(prefix, last, e); err != nil {
			return listing.Page{}, nil, err
		}
		last = e.Key
		if len(e.Key) == len(prefix) {
			page.Self = &e
			continue
		}
		page.Entries = append(page.Entries, e)
	}
	if len(page.Entries) > limit {
		return listing.Page{}, nil, badPage("%d children, %d asked for", len(page.Entries), limit)
	}
	return page, last, nil
}

// checkEntry refuses an entry that is not one child of prefix, or the
// entry at exactly it, sorting after last (nil before any key). A child
// directory or common prefix is one name and a "/", any other child one
// name.
func (s *Source) checkEntry(prefix, last []byte, e listing.Entry) error {
	k := e.Key
	if !bytes.HasPrefix(k, prefix) {
		return badPage("a key outside the prefix: %q", k)
	}
	rest := k[len(prefix):]
	dir := e.Kind == listing.KindPrefix || e.Kind == listing.KindDir
	self := len(rest) == 0 && e.Kind != listing.KindPrefix
	switch {
	case bytes.IndexByte(k, 0) >= 0:
		return badPage("a key holding a NUL byte: %q", k)
	case s.utf8Keys && !utf8.Valid(k):
		return badPage("a key that is not UTF-8: %q", k)
	case len(k) == 0:
		return badPage("an empty key")
	case dir && !self && (len(rest) == 0 || rest[len(rest)-1] != '/' || bytes.IndexByte(rest[:len(rest)-1], '/') >= 0):
		return badPage("a directory or common prefix that is not one child of the prefix: %q", k)
	case !dir && bytes.IndexByte(rest, '/') >= 0:
		return badPage("an entry below a child of the prefix: %q", k)
	case last != nil && bytes.Compare(k, last) <= 0:
		return badPage("a key that does not sort after the one before it: %q after %q", k, last)
	}
	return nil
}

// checkContinuation refuses a truncated answer's continuation token that
// is missing, or that repeats sent, the one it answered.
func checkContinuation(sent, next string) error {
	switch {
	case next == "":
		return badPage("a truncated answer without a continuation token")
	case next == sent:
		return badPage("a continuation token that repeats the one sent")
	}
	return nil
}

func badPage(format string, args ...any) error {
	return fmt.Errorf("%w: %s", listing.ErrInvalidPage, fmt.Sprintf(format, args...))
}

// tokenVersion begins every page token: "live1.", the base64url of the
// binding, then of the level's last key and of the store's continuation
// token, each after a ".". A token of another version is refused.
const tokenVersion = "live1"

// cursor is where a level goes on: after its last key so far (nil before
// any), from the store's continuation token ("" at the start).
type cursor struct {
	last  []byte
	store string
}

// binding ties a token to one location and one prefix. Limit and the
// bounds of a page's work may change from page to page. Tokens are not
// signed: the binding detects a token used with another source or prefix,
// not a forged one.
func binding(location string, prefix []byte) string {
	h := sha256.New()
	h.Write([]byte("swath live page token\x00"))
	h.Write([]byte(location))
	h.Write([]byte{0})
	h.Write(prefix)
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil)[:16])
}

func encodeToken(bind string, c cursor) string {
	enc := base64.RawURLEncoding
	return strings.Join([]string{tokenVersion, bind, enc.EncodeToString(c.last), enc.EncodeToString([]byte(c.store))}, ".")
}

// decodeToken returns where the token of prefix's level, bound as bind,
// goes on: the start for the empty token. A token of another version or
// binding, whose last key is not in the level, or that carries no
// continuation token of the store, is ErrInvalidToken.
func decodeToken(prefix []byte, bind, token string) (cursor, error) {
	if token == "" {
		return cursor{}, nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != tokenVersion {
		return cursor{}, badToken("not a token of this version")
	}
	if parts[1] != bind {
		return cursor{}, badToken("issued for another source or prefix")
	}
	last, lerr := base64.RawURLEncoding.DecodeString(parts[2])
	store, serr := base64.RawURLEncoding.DecodeString(parts[3])
	switch {
	case lerr != nil || serr != nil:
		return cursor{}, badToken("it does not decode")
	case len(store) == 0:
		return cursor{}, badToken("it carries no continuation token")
	case len(last) > 0 && !bytes.HasPrefix(last, prefix):
		return cursor{}, badToken("its last key is outside the prefix")
	}
	c := cursor{store: string(store)}
	if len(last) > 0 {
		c.last = last
	}
	return c, nil
}

func badToken(reason string) error {
	return fmt.Errorf("%w: %s", listing.ErrInvalidToken, reason)
}

// errTimeBudget is the cause of a page's context ending at its
// MaxDuration.
var errTimeBudget = errors.New("swath: the page's time budget ended")

// withBudget bounds ctx by a page's MaxDuration, when it sets one.
func withBudget(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeoutCause(ctx, d, errTimeBudget)
}

// timeUp reports whether op, derived from ctx by withBudget, ended at its
// time budget while ctx itself goes on.
func timeUp(ctx, op context.Context) bool {
	return ctx.Err() == nil && errors.Is(context.Cause(op), errTimeBudget)
}
