// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/redact"
)

// DefaultChunkSize is the bytes a resumable upload sends per request, and
// buffers in memory (one chunk an upload, held for a retry).
const DefaultChunkSize = 8 << 20

// ChunkBytes is the chunk a resumable upload of chunk size n sends and
// buffers: n rounded up to a multiple of 256 KiB, as the SDK rounds it
// (googleapi.MinUploadChunkSize); DefaultChunkSize for 0 or less. It is
// the upload's memory a caller admits.
func ChunkBytes(n int) int {
	if n <= 0 {
		return DefaultChunkSize
	}
	const unit = googleapi.MinUploadChunkSize
	return (n + unit - 1) / unit * unit
}

// DefaultMaxAttempts sets the retry allowance: MaxAttempts each for a
// metadata read or a delete; for an upload, MaxAttempts-1 retries for its
// first request and as many more for each chunk sent (WriteOptions).
const DefaultMaxAttempts = 5

// DefaultChunkTransferTimeout bounds one chunk request of a resumable
// upload: 8 MiB at under 70 KB/s.
const DefaultChunkTransferTimeout = 2 * time.Minute

// WriteOptions configure a Writer.
type WriteOptions struct {
	// ChunkSize is a resumable upload's request size and buffer (0:
	// DefaultChunkSize). The SDK rounds it up to a multiple of 256 KiB. An
	// object smaller than one chunk is created in one request.
	ChunkSize int
	// MaxAttempts sets the retry allowance (0: DefaultMaxAttempts). The
	// SDK's upload loops count no attempts, so an upload's allowance is
	// cumulative: MaxAttempts-1 retries for its first request and as many
	// more for each chunk it has sent, spent wherever its failures fall
	// (one chunk may use more than MaxAttempts-1 of them); a chunk's
	// retries also stop at ChunkRetryDeadline. The SDK retries what its policy deems transient
	// (408, 429, 5xx, connection resets), including requests made under a
	// generation precondition, which are idempotent.
	MaxAttempts int
	// ChunkRetryDeadline bounds the time one chunk is retried (0: the
	// SDK's, 32 seconds).
	ChunkRetryDeadline time.Duration
	// ChunkTransferTimeout bounds one chunk request, so a stalled request
	// is retried (within ChunkRetryDeadline) instead of waited on (0:
	// DefaultChunkTransferTimeout).
	ChunkTransferTimeout time.Duration
	// Retried, if set, is called with each failed attempt the SDK's policy
	// would retry, and the operation it belongs to (a caller's account of
	// retries and their classes). It must return quickly.
	Retried func(op string, err error)
}

// NewWriter returns a Writer of the objects of bucket through client, which
// carries the caller's credentials (with a write scope), endpoint and
// transport. Uploads are resumable in chunks of o.ChunkSize; CreateOnly is
// ifGenerationMatch=0, which the service checks when the object is created.
// Content is stored as given: no content-type sniffing, no transcoding.
func NewWriter(client *storage.Client, bucket string, o WriteOptions) objfile.Writer {
	o.ChunkSize = ChunkBytes(o.ChunkSize)
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}
	if o.ChunkTransferTimeout <= 0 {
		o.ChunkTransferTimeout = DefaultChunkTransferTimeout
	}
	return writer{bucket: client.Bucket(bucket), o: o}
}

type writer struct {
	bucket *storage.BucketHandle
	o      WriteOptions
}

// object is name's handle with the writer's retry policy. budget, if set,
// is asked before each retry: the SDK's upload loops (a single-request
// creation, a session's chunks) do not count attempts themselves.
func (w writer) object(name string, budget func() bool) *storage.ObjectHandle {
	retried := w.o.Retried
	return w.bucket.Object(name).Retryer(storage.WithMaxAttempts(w.o.MaxAttempts), storage.WithPolicy(storage.RetryIdempotent),
		storage.WithErrorFuncWithContext(func(err error, rc *storage.RetryContext) bool {
			if !storage.ShouldRetry(err) || budget != nil && !budget() {
				return false
			}
			if retried != nil {
				op := "upload"
				if rc != nil {
					op = rc.Operation
				}
				retried(op, err)
			}
			return true
		}))
}

func (w writer) NewWriter(ctx context.Context, name string, c objfile.Condition) (objfile.Upload, error) {
	u := &upload{name: name}
	// An upload's retries: MaxAttempts-1 a request on average, over the
	// session's start and each chunk sent so far (a chunk is also bounded
	// by ChunkRetryDeadline).
	chunk, perRequest := int64(w.o.ChunkSize), int64(w.o.MaxAttempts-1)
	obj := w.object(name, func() bool {
		// A retried attempt failed in a way that may have reached the
		// service: whatever the last attempt says, the completion is
		// unresolved unless it succeeds (completionError).
		u.uncertain.Store(true)
		return u.retries.Add(1) <= perRequest*(1+u.written.Load()/chunk)
	})
	switch c {
	case objfile.CreateOnly:
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	default:
		return nil, fmt.Errorf("objfile/gcs: write %s: unsupported condition %d", name, c)
	}
	ctx, cancel := context.WithCancel(ctx)
	sw := obj.NewWriter(ctx)
	sw.ChunkSize = w.o.ChunkSize
	sw.ChunkRetryDeadline = w.o.ChunkRetryDeadline
	sw.ChunkTransferTimeout = w.o.ChunkTransferTimeout
	sw.ContentType = "application/octet-stream" // no sniffing
	u.w, u.cancel = sw, cancel
	return u, nil
}

// upload is one object's resumable upload. The SDK's Writer buffers a
// chunk, sends it from a goroutine of its own and blocks Write while the
// previous chunk is in flight: memory is one chunk, and a slow store holds
// the writer back.
type upload struct {
	name   string
	w      *storage.Writer
	cancel context.CancelFunc

	written, retries atomic.Int64 // bytes written; retried attempts (read on the SDK's goroutine)
	uncertain        atomic.Bool  // an attempt was retried after an ambiguous failure

	// wmu serializes the SDK writer's Write and Close: Abort cancels the
	// context first, which ends a Write blocked on the network, then takes
	// wmu to join the SDK's goroutine.
	wmu sync.Mutex

	mu      sync.Mutex
	state   int // 0 open, 1 closing or closed, 2 aborted
	receipt objfile.Receipt
	done    bool
}

func (u *upload) Write(p []byte) (int, error) {
	u.wmu.Lock()
	defer u.wmu.Unlock()
	u.mu.Lock()
	state := u.state
	u.mu.Unlock()
	if state != 0 {
		return 0, redact.URLs(fmt.Errorf("objfile/gcs: write %s: upload closed or aborted", u.name))
	}
	n, err := u.w.Write(p)
	u.written.Add(int64(n))
	if err != nil {
		// A session start or a middle chunk creates no object: a failed
		// Write is never unresolved. The session's precondition may fail
		// here (ErrExists).
		if status(err) == http.StatusPreconditionFailed {
			err = fmt.Errorf("%w: %w", objfile.ErrExists, err)
		}
		err = redact.URLs(&fs.PathError{Op: "write", Path: u.name, Err: err})
	}
	return n, err
}

func (u *upload) Close() error {
	u.mu.Lock()
	if u.state != 0 {
		u.mu.Unlock()
		return redact.URLs(fmt.Errorf("objfile/gcs: write %s: upload already closed or aborted", u.name))
	}
	u.state = 1
	u.mu.Unlock()
	u.wmu.Lock()
	err := u.w.Close()
	u.wmu.Unlock()
	if err == nil {
		a := u.w.Attrs()
		switch {
		case a == nil:
			err = fmt.Errorf("%w: the service returned no object", objfile.ErrUnresolved)
		case a.Size != u.written.Load():
			err = fmt.Errorf("the service stored %d bytes of %d written", a.Size, u.written.Load())
		default:
			u.mu.Lock()
			u.receipt, u.done = objfile.Receipt{Name: u.name, Size: a.Size, Identity: strconv.FormatInt(a.Generation, 10)}, true
			u.mu.Unlock()
		}
	}
	u.cancel()
	if err != nil {
		return completionError(u.name, err, u.uncertain.Load())
	}
	return nil
}

// Abort cancels the upload before Close: the session's request is
// cancelled and no object is created, since only Close sends the request
// that creates it. A session the service already holds expires (GCS keeps
// an unfinished resumable session for a week); its URI never leaves this
// process. It may run while a Write is blocked on the network, which the
// cancellation ends.
func (u *upload) Abort() error {
	u.mu.Lock()
	state := u.state
	if state == 0 {
		u.state = 2
	}
	u.mu.Unlock()
	u.cancel()
	if state == 0 && u.written.Load() > 0 {
		u.wmu.Lock()
		_ = u.w.Close() // waits for the SDK's goroutine; fails with the cancellation
		u.wmu.Unlock()
	}
	return nil
}

func (u *upload) Receipt() (objfile.Receipt, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.receipt, u.done
}

func (w writer) PutIfAbsent(ctx context.Context, name string, data []byte) (objfile.Receipt, error) {
	u, err := w.NewWriter(ctx, name, objfile.CreateOnly)
	if err != nil {
		return objfile.Receipt{}, err
	}
	if _, err := u.Write(data); err != nil {
		_ = u.Abort()
		return objfile.Receipt{}, err
	}
	if err := u.Close(); err != nil {
		return objfile.Receipt{}, err
	}
	r, _ := u.Receipt()
	return r, nil
}

func (w writer) DeleteIf(ctx context.Context, name, identity string) error {
	return redact.URLs(w.deleteIf(ctx, name, identity))
}

func (w writer) deleteIf(ctx context.Context, name, identity string) error {
	gen, err := strconv.ParseInt(identity, 10, 64)
	if err != nil || gen <= 0 {
		return &fs.PathError{Op: "delete", Path: name, Err: fmt.Errorf("%w: identity %q is not a generation", fs.ErrInvalid, identity)}
	}
	err = w.object(name, nil).Generation(gen).If(storage.Conditions{GenerationMatch: gen}).Delete(ctx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrObjectNotExist):
		// Gone, or another generation is live: tell them apart.
		if a, aerr := w.bucket.Object(name).Attrs(ctx); aerr == nil && a.Generation != gen {
			err = fmt.Errorf("%w: generation %d is live, not %d", objfile.ErrObjectChanged, a.Generation, gen)
		} else {
			err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
		}
	case status(err) == http.StatusPreconditionFailed:
		err = fmt.Errorf("%w: %w", objfile.ErrObjectChanged, err)
	}
	return &fs.PathError{Op: "delete", Path: name, Err: err}
}

// completionError classifies a failed completion (the request that creates
// the object): a failed precondition is objfile.ErrExists; a service answer
// that refuses the request (4xx but 408 and 429) leaves no object, unless
// an earlier attempt of the upload failed ambiguously (uncertain: the SDK
// retried it), which the refusal does not undo; anything else (a transport
// failure, a timeout, a cancellation in flight, a server error, a throttle
// a proxy may have answered for a request still in flight) is
// objfile.ErrUnresolved. The text is redacted.
func completionError(name string, err error, uncertain bool) error {
	switch code := status(err); {
	case errors.Is(err, objfile.ErrUnresolved):
	case code == http.StatusPreconditionFailed:
		err = fmt.Errorf("%w: %w", objfile.ErrExists, err)
	case code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests && !uncertain:
	default:
		err = fmt.Errorf("%w: %w", objfile.ErrUnresolved, err)
	}
	return redact.URLs(&fs.PathError{Op: "write", Path: name, Err: err})
}

// status is the HTTP status of a service error, 0 for another error.
func status(err error) int {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code
	}
	return 0
}
