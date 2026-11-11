// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"context"
	"errors"
	"io"
	"strings"
)

// Condition is the service-enforced precondition of a write.
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
// ConditionalRequestConflict). Unless the error also matches ErrUnresolved,
// this write created no object and can be retried from its first byte.
var ErrConflict = errors.New("objfile: a concurrent conditional write of the name conflicted")

// Receipt records a confirmed creation: the caller's name, the stored size and
// the provider identity described by Object.Identity. An ETag identity does not
// distinguish separate creations that share that ETag.
type Receipt struct {
	Name     string
	Size     int64
	Identity string
}

// ErrUnresolved reports a write whose outcome is unknown: the request failed
// in transport, timed out, or was answered with a server error, so the
// store may have applied it, or may still apply it. A caller that must
// know reads the name back, and takes absence as proof of nothing while
// the request could still be in flight. This uncertainty applies even when
// another sentinel also matches: an ambiguous attempt followed by a conflict
// can match both ErrUnresolved and ErrConflict.
var ErrUnresolved = errors.New("objfile: the write's outcome is unknown")

// Upload writes one object's bytes in order. Close sends the final creation
// request; a nil result confirms that the service created the object and makes
// Receipt valid. A failed Close can also leave an object: ErrExists may follow
// a lost success response, and ErrUnresolved means creation may have happened
// or may still happen. Read back to check whether the intended content is
// present; a failed Close does not establish ownership. Absence is inconclusive
// while a request could still be in flight.
//
// Abort before Close prevents creation. After Close has sent its request,
// Abort cannot undo a creation or settle an unresolved outcome; after a
// successful Close it does nothing. Provider-specific temporary resources may
// need cleanup even when no object was created. Use an Upload from one
// goroutine at a time, except that Abort may cancel an in-progress Write from
// another goroutine.
type Upload interface {
	io.Writer
	Close() error
	Abort() error
	// Receipt returns the confirmed creation after a successful Close;
	// ok is false otherwise.
	Receipt() (r Receipt, ok bool)
}

// Writer creates objects by name, honoring ctx, and conditionally deletes them.
// Store packages redact request URLs in errors, preserving errors.Is and
// errors.As. Callers can use RedactError for errors they add themselves.
type Writer interface {
	// NewWriter starts an upload of name under condition c. Bytes are sent
	// as they are written, in chunks the store's package bounds; Close sends
	// the final request that creates the object. See Upload for failed Close
	// and Abort outcomes.
	NewWriter(ctx context.Context, name string, c Condition) (Upload, error)
	// PutIfAbsent creates name holding data under CreateOnly, using one request
	// where the store allows. Its errors have the same outcome semantics as
	// Upload.Close, including ErrExists after a lost success response.
	PutIfAbsent(ctx context.Context, name string, data []byte) (Receipt, error)
	// DeleteIf asks the service to delete the version or generation named by
	// identity, or the current object under an ETag match. A named version
	// can be deleted while a newer version remains current. An ETag match can
	// delete a replacement with the same ETag; S3 ETags are not unique creation
	// identifiers. The service must honor the supplied delete condition.
	// A reported identity mismatch wraps ErrObjectChanged. An absent target
	// may succeed or return an error wrapping fs.ErrNotExist, depending on the
	// provider; success does not prove the target existed.
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
