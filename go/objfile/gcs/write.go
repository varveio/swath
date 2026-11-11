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
)

// DefaultChunkSize is the bytes a resumable upload sends per request, and
// buffers in memory (one chunk an upload, held for a retry).
const DefaultChunkSize = 8 << 20

// DefaultMaxAttempts bounds each request's attempts, the SDK's retries
// included.
const DefaultMaxAttempts = 5

// WriteOptions configure a Writer.
type WriteOptions struct {
	// ChunkSize is a resumable upload's request size and buffer (0:
	// DefaultChunkSize). The SDK rounds it up to a multiple of 256 KiB. An
	// object smaller than one chunk is created in one request.
	ChunkSize int
	// MaxAttempts bounds each request's attempts (0: DefaultMaxAttempts).
	// The SDK's upload loops count no attempts, so an upload is given
	// MaxAttempts-1 retries for its first request and as many more for
	// each chunk it has sent; a chunk's retries also stop at
	// ChunkRetryDeadline. The SDK retries what its policy deems transient
	// (408, 429, 5xx, connection resets), including requests made under a
	// generation precondition, which are idempotent.
	MaxAttempts int
	// ChunkRetryDeadline bounds the time one chunk is retried (0: the
	// SDK's, 32 seconds).
	ChunkRetryDeadline time.Duration
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
	if o.ChunkSize <= 0 {
		o.ChunkSize = DefaultChunkSize
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
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
	n      int64 // bytes written

	written, retries atomic.Int64 // read by the retry budget, on the SDK's goroutine

	mu      sync.Mutex
	state   int // 0 open, 1 closing or closed, 2 aborted
	receipt objfile.Receipt
	done    bool
}

func (u *upload) Write(p []byte) (int, error) {
	n, err := u.w.Write(p)
	u.n += int64(n)
	u.written.Store(u.n)
	if err != nil {
		err = writeError(u.name, err)
	}
	return n, err
}

func (u *upload) Close() error {
	u.mu.Lock()
	if u.state != 0 {
		u.mu.Unlock()
		return fmt.Errorf("objfile/gcs: write %s: upload already closed or aborted", u.name)
	}
	u.state = 1
	u.mu.Unlock()
	err := u.w.Close()
	if err == nil {
		a := u.w.Attrs()
		switch {
		case a == nil:
			err = errors.New("the service returned no object")
		case a.Size != u.n:
			err = fmt.Errorf("the service stored %d bytes of %d written", a.Size, u.n)
		default:
			u.mu.Lock()
			u.receipt, u.done = objfile.Receipt{Name: u.name, Size: a.Size, Identity: strconv.FormatInt(a.Generation, 10)}, true
			u.mu.Unlock()
		}
	}
	u.cancel()
	if err != nil {
		return writeError(u.name, err)
	}
	return nil
}

// Abort cancels the upload: its session request is cancelled and no
// object is created. A session the service already holds expires (GCS
// keeps an unfinished resumable session for a week); its URI never leaves
// this process.
func (u *upload) Abort() error {
	u.mu.Lock()
	state := u.state
	if state == 0 {
		u.state = 2
	}
	u.mu.Unlock()
	u.cancel()
	if state == 0 && u.n > 0 {
		_ = u.w.Close() // waits for the SDK's goroutine; fails with the cancellation
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

// writeError marks a failed precondition as objfile.ErrExists.
func writeError(name string, err error) error {
	if status(err) == http.StatusPreconditionFailed {
		err = fmt.Errorf("%w: %w", objfile.ErrExists, err)
	}
	return &fs.PathError{Op: "write", Path: name, Err: err}
}

// status is the HTTP status of a service error, 0 for another error.
func status(err error) int {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code
	}
	return 0
}
