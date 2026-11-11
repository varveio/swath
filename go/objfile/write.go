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
// ifGenerationMatch=0, S3 If-None-Match: *. A write that finds one fails
// with ErrExists.
const CreateOnly Condition = 1

// ErrExists reports a CreateOnly write that found an object at its name
// (the store's precondition failed). After a lost response the object
// found may be the write's own: a caller that must know reads it back.
var ErrExists = errors.New("objfile: an object already exists at the name")

// Receipt identifies an object a write created: its name as the caller
// gave it, its size and its Identity (the GCS generation, as Object's
// Identity names it).
type Receipt struct {
	Name     string
	Size     int64
	Identity string
}

// Upload is one object being written. Write takes its bytes in order; Close
// completes it, and only a Close that returns nil leaves an object, whose
// Receipt is then valid. Abort ends it without an object (after a failed
// Close, or instead of one); Abort after a successful Close does nothing.
// An Upload is used by one goroutine at a time; Abort may be called from
// another to cancel it.
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
	// where the store allows: ErrExists when an object is already there.
	PutIfAbsent(ctx context.Context, name string, data []byte) (Receipt, error)
	// DeleteIf deletes name only if its current version is identity: one
	// that is gone fails with an error wrapping fs.ErrNotExist, one
	// replaced since with ErrObjectChanged.
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
