// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/reader/listing"
)

// sortBound is the largest level a live page sorts: what one List Blobs
// answer holds at most.
const sortBound = client.MaxPageSize

// Live is a container opened for one-level listings and lookups
// (OpenLive): the built client, with its credentials and admission, no
// namespace admission and no range engine. It is a live.Store, safe for
// concurrent use.
//
// Its pages are in byte order. A level of a flat namespace comes in UTF-16
// code-unit order, a hierarchical namespace's in that namespace's own, and
// a hierarchical namespace lists each directory twice: as the blob that
// stands for it (client.Directory) and as its prefix. A level that one
// answer holds whole is sorted, its directories each one prefix; a level
// longer than one answer is paged in Azure's order, and is refused
// (listing.ErrUnsupported) once it shows an order that is not byte order
// or a directory's blob.
type Live struct {
	container string
	client    *client.Client
	location  string
}

var _ live.Store = (*Live)(nil)

// OpenLive builds the client of opt.Account as New does, which may load
// credentials, and sends nothing: no namespace admission. It does not use
// opt.Prefix, the range engine's policy or a supplied Client. Its errors
// match a class of package listing (objhttp.ListingClass), or are ctx's
// own.
func OpenLive(ctx context.Context, opt Options) (*Live, error) {
	opt.Client = nil
	p, _, err := connect[Object](ctx, opt)
	if err != nil {
		return nil, failure(ctx, err)
	}

	location := strings.Join([]string{"azure", opt.Account, opt.Container, objhttp.CanonicalRoute(p.route)}, "\x00")
	return &Live{container: opt.Container, client: p.client.(*client.Client), location: location}, nil
}

// Location identifies the container by its account, its name and the
// route its requests go to (live.Store).
func (l *Live) Location() string { return l.location }

// cursor is where a level goes on, as a live page token's store token
// carries it.
type cursor struct {
	// sorted: the level is served from one answer, sorted.
	sorted bool
	// last is the level's last key so far, nil before any.
	last []byte
	// from and marker continue a chain of answers in Azure's order: the
	// startFrom the chain began at ("" at the level's start) and its next
	// marker ("" to begin a chain at last).
	from, marker string
}

// The cursor's two forms: a sorted level's, and a chain's.
const (
	sortedCursor = "s"
	chainCursor  = "c"
)

// encode is the cursor as a store token: its fields joined by NUL, which
// no key holds.
func (c cursor) encode() (string, error) {
	if c.sorted {
		return sortedCursor + "\x00" + string(c.last), nil
	}
	if strings.Contains(c.marker, "\x00") {
		return "", fmt.Errorf("%w: a NextMarker holding a NUL byte", listing.ErrInvalidPage)
	}
	return strings.Join([]string{chainCursor, c.from, c.marker, string(c.last)}, "\x00"), nil
}

// decodeCursor reads a store token; "" is the level's start.
func decodeCursor(token string) (cursor, error) {
	if token == "" {
		return cursor{}, nil
	}

	f := strings.Split(token, "\x00")
	var c cursor
	switch {
	case f[0] == sortedCursor && len(f) == 2:
		c = cursor{sorted: true, last: []byte(f[1])}
	case f[0] == chainCursor && len(f) == 4:
		c = cursor{from: f[1], marker: f[2], last: []byte(f[3])}
	default:
		return cursor{}, fmt.Errorf("%w: not an Azure Blob cursor", listing.ErrInvalidToken)
	}
	if len(c.last) == 0 {
		c.last = nil
	}
	return c, nil
}

// List makes one List Blobs request with the delimiter "/", the client's
// retries included, and serves at most max entries of the level after the
// cursor in token, in byte order (live.Store; Live says how).
func (l *Live) List(ctx context.Context, prefix []byte, token string, max int) (*live.Level, error) {
	cur, err := decodeCursor(token)
	if err != nil {
		return nil, err
	}

	in := &client.ListInput{Container: l.container, Prefix: string(prefix), Delimiter: "/", AnyOrder: true,
		Include: client.Include{Metadata: true}}
	switch {
	case cur.sorted || token == "":
		in.MaxResults = sortBound
	case cur.marker == "":
		// a chain begins at last, which the answer lists again
		in.StartFrom, in.MaxResults = string(cur.last), min(max+1, sortBound)
	default:
		in.StartFrom, in.Marker, in.MaxResults = cur.from, cur.marker, min(max, sortBound)
	}

	out, err := l.client.List(ctx, in)
	if err != nil {
		return nil, failure(ctx, err)
	}
	entries, dirs, err := children(prefix, out.Items)
	if err != nil {
		return nil, err
	}

	// A whole level is sorted unless it is in byte order already, which a
	// chain then continues at a request of max entries, not of the level.
	whole := (cur.sorted || token == "") && out.NextMarker == ""
	if whole && (cur.sorted || dirs || !ascending(entries)) {
		return serveSorted(entries, cur.last, max), nil
	}
	switch {
	case cur.sorted:
		return nil, fmt.Errorf("%w: Azure Blob container %q: the level of %q, which is not in byte order, no longer fits one answer of %d entries",
			listing.ErrUnsupported, l.container, prefix, sortBound)
	case dirs:
		return nil, fmt.Errorf("%w: Azure Blob container %q: the level of %q holds a hierarchical namespace's directories and more than one answer of %d entries",
			listing.ErrUnsupported, l.container, prefix, sortBound)
	}
	return serveChain(l.container, prefix, in, out.NextMarker, entries, cur.last, max)
}

// children are a level's items as entries, in the answer's order: a blob
// as an object, a prefix as a prefix, and a directory's blob as its
// prefix (dirs says the answer held one). An item outside prefix is
// listing.ErrInvalidPage.
func children(prefix []byte, items []client.Item) (entries []listing.Entry, dirs bool, err error) {
	entries = make([]listing.Entry, 0, len(items))
	for i := range items {
		it := &items[i]
		if !strings.HasPrefix(it.Name, string(prefix)) {
			return nil, false, fmt.Errorf("%w: a key outside the prefix: %q", listing.ErrInvalidPage, it.Name)
		}
		switch {
		case it.Prefix:
			entries = append(entries, listing.Entry{Key: []byte(it.Name), Kind: listing.KindPrefix})
		case directory(it):
			dirs = true
			entries = append(entries, listing.Entry{Key: []byte(it.Name + "/"), Kind: listing.KindPrefix})
		default:
			entries = append(entries, object([]byte(it.Name), it.Size, it.LastModified, it.AccessTier))
		}
	}
	return entries, dirs, nil
}

// directory reports a listed blob that stands for a hierarchical
// namespace's directory: its metadata says so (client.Directory), and its
// name, as a hierarchical namespace's names never do, does not end in "/".
func directory(it *client.Item) bool {
	if it.Metadata == "" || strings.HasSuffix(it.Name, "/") {
		return false
	}
	var m map[string]*string
	if json.Unmarshal([]byte(it.Metadata), &m) != nil {
		return false
	}
	return client.Directory(m)
}

// object is an object's entry: its size, its last-modified time when
// known (micros, Unix microseconds) and its access tier when answered.
func object(key []byte, size, micros int64, tier string) listing.Entry {
	e := listing.Entry{Key: key, Kind: listing.KindObject, Size: &size}
	if micros != 0 {
		t := time.UnixMicro(micros).UTC()
		e.LastModified = &t
	}
	if tier != "" {
		e.StorageClass = &tier
	}
	return e
}

// serveSorted serves a whole level, entries in any order: sorted, a key
// listed twice (a directory's blob and its prefix) once, then at most max
// entries after last. A level that goes on carries a sorted cursor.
func serveSorted(entries []listing.Entry, last []byte, max int) *live.Level {
	slices.SortStableFunc(entries, func(a, b listing.Entry) int { return bytes.Compare(a.Key, b.Key) })
	entries = slices.CompactFunc(entries, func(a, b listing.Entry) bool { return bytes.Equal(a.Key, b.Key) })
	if last != nil {
		at, found := slices.BinarySearchFunc(entries, last, func(e listing.Entry, k []byte) int { return bytes.Compare(e.Key, k) })
		if found {
			at++
		}
		entries = entries[at:]
	}

	n := min(len(entries), max)
	lvl := split(entries[:n])
	if n < len(entries) {
		lvl.Truncated = true
		lvl.Next, _ = cursor{sorted: true, last: entries[n-1].Key}.encode() // a sorted cursor always encodes
	}
	return lvl
}

// serveChain serves at most max entries of one answer of a chain in
// Azure's order, sent as in and continuing at next, after last: they must
// ascend in byte order, a key equal to the one before (the answer's first,
// at the chain's startFrom) dropped. An answer that does not ascend shows
// the level's order is not byte order, too late to sort:
// listing.ErrUnsupported. The level goes on from the next marker once the
// answer is served whole, else from a chain begun at the last key served.
func serveChain(container string, prefix []byte, in *client.ListInput, next string, entries []listing.Entry, last []byte, max int) (*live.Level, error) {
	kept := entries[:0]
	prev := last
	for _, e := range entries {
		switch c := bytes.Compare(e.Key, prev); {
		case prev != nil && c == 0:
			continue
		case prev != nil && c < 0:
			return nil, fmt.Errorf("%w: Azure Blob container %q lists the level of %q in an order that is not byte order (%q after %q), "+
				"and it is longer than one answer of %d entries, which could be sorted", listing.ErrUnsupported, container, prefix, e.Key, prev, sortBound)
		}
		kept = append(kept, e)
		prev = e.Key
	}

	n := min(len(kept), max)
	lvl := split(kept[:n])
	c := cursor{last: last, from: in.StartFrom, marker: next}
	if n > 0 {
		c.last = kept[n-1].Key
	}
	if n < len(kept) {
		c.from, c.marker = string(c.last), ""
	}
	if n < len(kept) || next != "" {
		tok, err := c.encode()
		if err != nil {
			return nil, err
		}
		lvl.Truncated, lvl.Next = true, tok
	}
	return lvl, nil
}

// ascending reports entries whose keys ascend strictly in byte order.
func ascending(entries []listing.Entry) bool {
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			return false
		}
	}
	return true
}

// split is entries, in byte order, as a level: its objects and its
// prefixes.
func split(entries []listing.Entry) *live.Level {
	lvl := &live.Level{}
	for _, e := range entries {
		if e.Kind == listing.KindPrefix {
			lvl.Prefixes = append(lvl.Prefixes, e.Key)
			continue
		}
		lvl.Objects = append(lvl.Objects, e)
	}
	return lvl
}

// Lookup is Get Blob Properties of key (live.Store): present with what the
// answer says of the blob, or, for a hierarchical namespace's directory,
// present as its prefix. A 404 is absent only once the probe, a List Blobs
// of key's own prefix for one entry, shows the container is there and may
// be listed; a missing container and a failed probe leave the key unknown.
// Any other failure leaves it unknown too, with its class: a 403 is
// ErrAuth.
func (l *Live) Lookup(ctx context.Context, key []byte) (listing.LookupResult, error) {
	h, err := l.client.Properties(ctx, l.container, string(key))
	switch {
	case err == nil:
		return present(key, h), nil
	case ctx.Err() != nil:
		return listing.LookupResult{}, ctx.Err()
	case !notFound(err):
		return unknown(objhttp.Classed(err)), nil
	}

	_, err = l.client.List(ctx, &client.ListInput{Container: l.container, Prefix: string(key), MaxResults: 1})
	switch {
	case err == nil:
		return listing.LookupResult{State: listing.LookupAbsent}, nil
	case ctx.Err() != nil:
		return listing.LookupResult{}, ctx.Err()
	case notFound(err):
		return unknown(fmt.Errorf("%w: container %q: %w", listing.ErrNotFound, l.container, err)), nil
	}
	return unknown(objhttp.Classed(err)), nil
}

// present is the entry a Get Blob Properties answer describes.
func present(key []byte, h *client.Head) listing.LookupResult {
	if h.Directory && !bytes.HasSuffix(key, []byte("/")) {
		dir := append(bytes.Clone(key), '/')
		return listing.LookupResult{State: listing.LookupPresent, Entry: &listing.Entry{Key: dir, Kind: listing.KindPrefix}}
	}
	e := object(bytes.Clone(key), h.Size, h.LastModified, h.AccessTier)
	return listing.LookupResult{State: listing.LookupPresent, Entry: &e}
}

// notFound: Azure answered 404, for the blob or its container.
func notFound(err error) bool {
	var e *client.Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

func unknown(reason error) listing.LookupResult {
	return listing.LookupResult{State: listing.LookupUnknown, Reason: reason}
}

// failure is a failed call's error: ctx's own once it has ended, else err
// in its class.
func failure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return objhttp.Classed(err)
}

// Close closes the client's idle connections (live.Store).
func (l *Live) Close() error {
	l.client.CloseIdleConnections()
	return nil
}
