// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package swath

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

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// Page returns the next children of req.Prefix, one level by "/", in byte
// order (OrderBytes): one delimiter listing, the store's retries included,
// and no request per child. The prefix is any key prefix, taken
// literally: "a" holds both "ab" and the prefix "a/". A child prefix is a
// KindPrefix entry; the object at exactly the prefix comes back once, as
// the Self of the page that reads it, never as a child.
//
// Page checks every page before returning it, as package listing states:
// a key outside the prefix, below one of its children, holding a NUL, not
// UTF-8, or not after the key before it, across pages too, more entries
// than asked for, and a truncated answer whose continuation token is
// missing or repeats the one sent are ErrInvalidPage.
//
// A token is opaque and versioned. It carries the store's continuation
// token and the level's last key, and is bound to the source's location
// (the bucket and what decides which bucket of that name it is), to the
// prefix and to whether, and with what kinds, req.ChildTotals is set; one
// issued for another is ErrInvalidToken, before any request. A live source
// returns no child totals. req.MaxScanned never stops a page: it reads
// what it returns. When req.MaxDuration ends a page it holds no entry,
// its Next is req.Token, which asks again, and its Evidence.Stop is
// StopTimeBudget.
func (s *Source) Page(ctx context.Context, req listing.PageRequest) (listing.Page, error) {
	totals, err := checkPage(req)
	if err != nil {
		return listing.Page{}, err
	}
	bind := binding(s.store.Location(), req.Prefix, totals)
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

	page, last, err := level(req.Prefix, cur.last, lvl, req.Limit)
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

// checkPage refuses a request of the wrong form, before any request, and
// returns the binding of its child totals: nil when it asks for none,
// else a byte for each kind, 1 where they count it, so that one count
// spelt in two ways binds alike.
func checkPage(req listing.PageRequest) ([]byte, error) {
	switch {
	case bytes.IndexByte(req.Prefix, 0) >= 0:
		return nil, invalid("the prefix holds a NUL byte")
	case !utf8.Valid(req.Prefix):
		return nil, invalid("the prefix is not UTF-8")
	case req.Limit < 1:
		return nil, invalid("a page's Limit is positive")
	case req.MaxScanned < 0 || req.MaxDuration < 0:
		return nil, invalid("a negative bound")
	case req.ChildTotals == nil:
		return nil, nil
	}

	b := req.ChildTotals.Budget
	if b.MaxEntries < 0 || b.MaxDuration < 0 || b.MaxHeldBytes < 0 || b.MaxValueBytes < 0 {
		return nil, invalid("a negative bound")
	}
	kinds := req.ChildTotals.Kinds
	counted := make([]byte, listing.KindUnknown+1)
	for _, k := range kinds {
		if k == 0 || k > listing.KindUnknown {
			return nil, invalid("%v is no kind", k)
		}
		counted[k] = 1
	}
	if len(kinds) == 0 {
		for k := listing.KindObject; k <= listing.KindUnknown; k++ {
			counted[k] = 1
		}
	}
	counted[listing.KindPrefix] = 0 // never counted
	return counted, nil
}

// level checks one answer of the store against the level of prefix, after
// the level's last key so far (nil before any), and makes it a page of at
// most limit entries: the objects and common prefixes merged in byte
// order, and the object at exactly prefix as Self. It returns the page and
// the level's last key after it.
func level(prefix, last []byte, lvl *live.Level, limit int) (listing.Page, []byte, error) {
	page := listing.Page{Order: listing.OrderBytes}
	objects, prefixes := lvl.Objects, lvl.Prefixes
	if n := len(objects) + len(prefixes); n > limit {
		return listing.Page{}, nil, badPage("%d entries, %d asked for", n, limit)
	}

	page.Entries = make([]listing.Entry, 0, len(objects)+len(prefixes))
	for len(objects) > 0 || len(prefixes) > 0 {
		var e listing.Entry
		if len(prefixes) == 0 || len(objects) > 0 && bytes.Compare(objects[0].Key, prefixes[0]) < 0 {
			e, objects = objects[0], objects[1:]
		} else {
			e, prefixes = listing.Entry{Key: prefixes[0], Kind: listing.KindPrefix}, prefixes[1:]
		}
		if err := checkEntry(prefix, last, e); err != nil {
			return listing.Page{}, nil, err
		}
		last = e.Key
		if len(e.Key) == len(prefix) {
			page.Self = &e
			continue
		}
		page.Entries = append(page.Entries, e)
	}
	return page, last, nil
}

// checkEntry refuses an entry that is not one child of prefix, or the
// object at exactly it, sorting after last (nil before any key).
func checkEntry(prefix, last []byte, e listing.Entry) error {
	k := e.Key
	if !bytes.HasPrefix(k, prefix) {
		return badPage("a key outside the prefix: %q", k)
	}
	rest := k[len(prefix):]
	switch {
	case bytes.IndexByte(k, 0) >= 0:
		return badPage("a key holding a NUL byte: %q", k)
	case !utf8.Valid(k):
		return badPage("a key that is not UTF-8: %q", k)
	case len(k) == 0:
		return badPage("an empty key")
	case e.Kind == listing.KindPrefix && (len(rest) == 0 || rest[len(rest)-1] != '/' || bytes.IndexByte(rest[:len(rest)-1], '/') >= 0):
		return badPage("a common prefix that is not one child of the prefix: %q", k)
	case e.Kind != listing.KindPrefix && bytes.IndexByte(rest, '/') >= 0:
		return badPage("an object below a child of the prefix: %q", k)
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

// binding ties a token to one location, one prefix and one count of child
// totals (checkPage). Limit and the bounds of a page's work may change
// from page to page. Tokens are not signed: the binding detects a token
// used with another source, prefix or count, not a forged one.
func binding(location string, prefix, totals []byte) string {
	h := sha256.New()
	h.Write([]byte("swath live page token\x00"))
	h.Write([]byte(location))
	h.Write([]byte{0})
	h.Write(prefix)
	h.Write([]byte{0})
	h.Write(totals)
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
		return cursor{}, badToken("issued for another source, prefix or count of child totals")
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
