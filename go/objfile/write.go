// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"context"
	"errors"
	"io"
	"strings"
)

// The write side: an object is created whole, under a precondition, and is
// visible only once its upload completes. Nothing here renames, lists,
// overwrites or deletes an object the caller does not name by its identity.

// Condition is the precondition of a write.
type Condition int

// CreateOnly creates the object only if no object has the name: GCS
// ifGenerationMatch=0, S3 and Azure If-None-Match: *. A write that finds
// one fails with ErrExists.
const CreateOnly Condition = 1

// ErrExists reports a CreateOnly write that found an object at its name
// (the store's precondition failed). After a lost response the object
// found may be the write's own: a caller that must know reads it back.
var ErrExists = errors.New("objfile: an object already exists at the name")

// ErrConflict reports a CreateOnly write the store refused because another
// conditional write of the name was in progress (S3's 409
// ConditionalRequestConflict). No object was created by it; the write may
// be made again, from its first byte (an upload's sent bytes are gone).
var ErrConflict = errors.New("objfile: a concurrent conditional write of the name conflicted")

// Receipt identifies an object a write created: its name as the caller
// gave it, its size and its Identity (the GCS generation, as Object's
// Identity names it).
type Receipt struct {
	Name     string
	Size     int64
	Identity string
}

// ErrUnresolved reports a write whose outcome is unknown: the request failed
// in transport, timed out, or was answered with a server error, so the
// store may have applied it, or may still apply it. A caller that must
// know reads the name back, and takes absence as proof of nothing while
// the request could still be in flight.
var ErrUnresolved = errors.New("objfile: the write's outcome is unknown")

// Upload is one object being written. Write takes its bytes in order; Close
// completes it. A Close that returns nil leaves an object, whose Receipt is
// then valid. A Close that fails with ErrUnresolved may have left one, or
// may yet leave one (a request the store accepted can outlive the failed
// connection): only a read of the name settles it, and nothing the caller
// does to the Upload can prevent it. A Close that fails otherwise, and an
// Abort before Close, leave no object. Abort after a successful Close does
// nothing. An Upload is used by one goroutine at a time; Abort may be
// called from another to cancel it, while a Write is in progress.
type Upload interface {
	io.Writer
	Close() error
	Abort() error
	// Receipt is the created object's, after a successful Close (ok false
	// before).
	Receipt() (r Receipt, ok bool)
}

// Writer creates objects by name, honouring ctx, and deletes the ones it is
// given the identity of.
type Writer interface {
	// NewWriter starts an upload of name under condition c. Bytes are sent
	// as they are written, in chunks the store's package bounds; no object
	// exists until Close succeeds.
	NewWriter(ctx context.Context, name string, c Condition) (Upload, error)
	// PutIfAbsent creates name holding data, CreateOnly, in one request
	// where the store allows: ErrExists when an object is already there,
	// ErrUnresolved when the outcome is unknown.
	PutIfAbsent(ctx context.Context, name string, data []byte) (Receipt, error)
	// DeleteIf deletes name only if its current version is identity: one
	// that is gone fails with an error wrapping fs.ErrNotExist, one
	// replaced since with ErrObjectChanged.
	//
	// Every error a Writer or an Upload returns has passed through
	// RedactError: no request URL's query or user information is in its
	// text.
	DeleteIf(ctx context.Context, name, identity string) error
}

// RootWriter scopes w to the objects under prefix, as Root scopes an Opener:
// each name is checked with CheckPath and written at prefix + "/" + name.
// Receipts name objects as the caller gave them, without the prefix.
func RootWriter(w Writer, prefix string) Writer {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return rootWriter{w: w, prefix: prefix}
}

type rootWriter struct {
	w      Writer
	prefix string
}

func (r rootWriter) NewWriter(ctx context.Context, name string, c Condition) (Upload, error) {
	if err := CheckPath(name); err != nil {
		return nil, err
	}
	u, err := r.w.NewWriter(ctx, r.prefix+name, c)
	if err != nil {
		return nil, err
	}
	return rootUpload{Upload: u, name: name}, nil
}

func (r rootWriter) PutIfAbsent(ctx context.Context, name string, data []byte) (Receipt, error) {
	if err := CheckPath(name); err != nil {
		return Receipt{}, err
	}
	rc, err := r.w.PutIfAbsent(ctx, r.prefix+name, data)
	rc.Name = name
	return rc, err
}

func (r rootWriter) DeleteIf(ctx context.Context, name, identity string) error {
	if err := CheckPath(name); err != nil {
		return err
	}
	return r.w.DeleteIf(ctx, r.prefix+name, identity)
}

type rootUpload struct {
	Upload
	name string
}

func (u rootUpload) Receipt() (Receipt, bool) {
	r, ok := u.Upload.Receipt()
	if ok {
		r.Name = u.name
	}
	return r, ok
}
