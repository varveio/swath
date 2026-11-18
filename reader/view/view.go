// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"net/http"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// Inventory answers the questions of package listing from one opened
// dataset. It is immutable and safe for concurrent use. An inventory
// OpenDir returns owns its dataset, which Close closes; one Open returns
// does not, and the caller closes the reader.Dataset after its last call.
type Inventory struct {
	sp *seekparquet.Dataset
	// dataset is the dataset OpenDir opened; nil for Open's.
	dataset *reader.Dataset
	// rows says how a row of the dataset's schema becomes an Entry.
	rows rowShape
	// base is every answer's Evidence.
	base listing.Evidence
	// digest is the SHA-256 of the manifest's bytes, which binds tokens.
	digest [sha256.Size]byte
	// provable: the evidence proves absence inside its scope (Unlocated
	// is 0).
	provable bool
}

// OpenDir opens the dataset in the local directory dir, checks every
// part's schema (reader.Dataset.CheckSchema) and returns its inventory,
// which owns the dataset until Close. It refuses what Open refuses, and a
// failure to read the dataset is of a class of package listing, as a
// call's is.
func OpenDir(ctx context.Context, dir string) (*Inventory, error) {
	d, err := reader.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		return nil, failed(ctx, err)
	}
	if err := d.CheckSchema(ctx); err != nil {
		return nil, errors.Join(failed(ctx, err), d.Close())
	}
	inv, err := Open(d)
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	inv.dataset = d
	return inv, nil
}

// Close closes the dataset OpenDir opened. An inventory Open returned owns
// nothing, and Close does nothing.
func (inv *Inventory) Close() error {
	if inv.dataset == nil {
		return nil
	}
	return inv.dataset.Close()
}

// Open returns the inventory of d. It reads nothing. A dataset of versions
// is refused with listing.ErrUnsupported, as is one whose manifest records
// no scope; a manifest whose key is not the schema's unique key column is
// listing.ErrInvalidPage, with the *reader.SemanticError wrapped.
func Open(d *reader.Dataset) (*Inventory, error) {
	m := d.Metadata()
	spec, ok := reader.Schema(m.SchemaID)
	if !ok {
		return nil, fmt.Errorf("%w: schema id %q is not one swath writes", listing.ErrUnsupported, m.SchemaID)
	}
	if !spec.UniqueKeys {
		return nil, fmt.Errorf("%w: %s lists versions, whose keys repeat", listing.ErrUnsupported, spec.ID)
	}
	if key := m.Manifest.Key; key.Column != spec.Key.Name || !key.Unique {
		return nil, fmt.Errorf("%w: %w", listing.ErrInvalidPage, &reader.SemanticError{Kind: reader.KindSchema, Column: key.Column,
			Err: fmt.Errorf("the manifest's key; %s's is the unique column %s", spec.ID, spec.Key.Name)})
	}
	scope, ok := scopeOf(m)
	if !ok {
		return nil, fmt.Errorf("%w: the manifest records no scope", listing.ErrUnsupported)
	}

	inv := &Inventory{sp: d.SeekParquet(), rows: shapeOf(spec.ID), digest: sha256.Sum256(m.Manifest.Manifest)}
	inv.base = listing.Evidence{
		Scope:     scope,
		Unlocated: unlocated(m.Coverage),
	}
	if c := m.Coverage; c != nil {
		inv.base.Started, inv.base.Finished = c.ListingStartedAt, c.ListingFinishedAt
	}
	inv.provable = inv.base.Unlocated != nil && *inv.base.Unlocated == 0
	return inv, nil
}

// scopeOf is the manifest's scope as a key prefix: empty for the whole
// source. ok is false when the manifest does not state it.
func scopeOf(m reader.Metadata) (scope []byte, ok bool) {
	s := m.Scope
	switch {
	case s == nil || s.Kind == nil:
		return nil, false
	case *s.Kind == "whole":
		return nil, true
	case *s.Kind == "prefix" && s.Prefix != nil:
		return []byte(*s.Prefix), true
	}
	return nil, false
}

// unlocated is the count of entries the listing may lack in its scope: its
// unreadable entries, when it proves it reached the end of its scope with
// no filter and no exclusion; nil otherwise.
func unlocated(c *reader.Coverage) *int64 {
	switch {
	case c == nil || c.Complete == nil || c.StopReason == nil || c.Unreadable == nil:
		return nil
	case *c.StopReason != "end_of_scope" || *c.Unreadable < 0 || *c.Complete != (*c.Unreadable == 0):
		return nil
	case c.Filters == nil || len(c.Filters) != 0 || c.Exclusions == nil || len(c.Exclusions) != 0:
		return nil
	}
	n := *c.Unreadable
	return &n
}

// evidence is a copy of the inventory's evidence, the caller's to keep.
func (inv *Inventory) evidence() listing.Evidence {
	e := inv.base
	e.Started, e.Finished = clone(e.Started), clone(e.Finished)
	e.Scope = bytes.Clone(e.Scope)
	e.Unlocated = clone(e.Unlocated)
	return e
}

// inScope reports whether every key at or under key lies in the
// inventory's scope.
func (inv *Inventory) inScope(key []byte) bool {
	return bytes.HasPrefix(key, inv.base.Scope)
}

// errOutOfScope is the answer to a question outside the scope.
var errOutOfScope = wrap(listing.ErrOutOfScope, "the key lies outside it")

// wrap is an error of class, for reason.
func wrap(class error, reason string) error {
	return fmt.Errorf("%w: %s", class, reason)
}

// invalid is a request this package refuses.
func invalid(format string, args ...any) error {
	return wrap(listing.ErrInvalidRequest, fmt.Sprintf(format, args...))
}

// failed is the error a call that read the inventory returns for err:
// ctx's own once ctx has ended, else err in a class of package listing,
// with err wrapped beside it. Data the reader refuses (reader.IsRefusal),
// a missing manifest among it, is ErrInvalidPage; a refused access, or a
// store's answer of 401 or 403, ErrAuth; a feature or limit the reader
// does not support ErrUnsupported; any other failure, such as a missing
// file or an I/O or network error past the storage's retries,
// ErrUnavailable. ErrNotFound is only ever the listed location's. An error the view gave a class keeps it.
func failed(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var status interface{ HTTPStatusCode() int }
	var unsupported *seekparquet.UnsupportedError
	code := 0
	if errors.As(err, &status) {
		code = status.HTTPStatusCode()
	}

	class := listing.ErrUnavailable
	switch {
	case err == nil || classed(err):
		return err
	case reader.IsRefusal(err):
		class = listing.ErrInvalidPage
	case errors.Is(err, fs.ErrPermission) || code == http.StatusUnauthorized || code == http.StatusForbidden:
		class = listing.ErrAuth
	case errors.As(err, &unsupported) || errors.Is(err, seekparquet.ErrLimitExceeded):
		class = listing.ErrUnsupported
	}
	return fmt.Errorf("%w: %w", class, err)
}

// classed reports whether err has a class the view gives its own errors.
func classed(err error) bool {
	for _, class := range []error{listing.ErrInvalidRequest, listing.ErrInvalidToken, listing.ErrNotFound, listing.ErrNotDirectory,
		listing.ErrUnsupported, listing.ErrInvalidPage, listing.ErrOutOfScope} {
		if errors.Is(err, class) {
			return true
		}
	}
	return false
}

// checkPrefix refuses a prefix of the wrong form: one holding a NUL, a
// local one that is not empty or a directory key.
func (inv *Inventory) checkPrefix(prefix []byte) error {
	if bytes.IndexByte(prefix, 0) >= 0 {
		return invalid("the prefix holds a NUL byte")
	}
	if inv.rows.local && len(prefix) > 0 && (prefix[len(prefix)-1] != '/' || prefix[0] == '/') {
		return invalid("a local prefix is empty or a directory's key, relative to the root and ending in /")
	}
	return nil
}

// checkKey refuses a key of the wrong form: empty, holding a NUL, or a
// local one that is not relative to the root.
func (inv *Inventory) checkKey(key []byte) error {
	switch {
	case len(key) == 0:
		return invalid("the key is empty")
	case bytes.IndexByte(key, 0) >= 0:
		return invalid("the key holds a NUL byte")
	case inv.rows.local && key[0] == '/':
		return invalid("a local key is relative to the root")
	}
	return nil
}

func clone[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
