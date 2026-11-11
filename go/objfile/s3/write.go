// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/redact"
)

// S3's multipart limits: every part but the last at least MinPartSize, at
// most MaxPartSize, and at most MaxParts parts an object.
const (
	MinPartSize = 5 << 20
	MaxPartSize = 5 << 30
	MaxParts    = 10000
)

// maxParts is MaxParts; a test lowers it.
var maxParts = MaxParts

// DefaultPartSize is the bytes an upload buffers and sends as one part.
const DefaultPartSize = 8 << 20

// DefaultMaxAttempts bounds each request's attempts, the first included.
const DefaultMaxAttempts = 5

// DefaultAbortTimeout bounds the abort of a multipart upload, which runs
// under a context of its own: the upload's may be the one cancelled.
const DefaultAbortTimeout = 30 * time.Second

// WriteOptions configure a Writer.
type WriteOptions struct {
	// PartSize is an upload's buffer and the size of each part but the
	// last (0: DefaultPartSize); MinPartSize to MaxPartSize. An object of
	// PartSize bytes or fewer is created by one PutObject; a larger one is
	// a multipart upload, so the largest object is MaxParts × PartSize.
	PartSize int64
	// MinPartSize is the store's smallest part (0: S3's, MinPartSize).
	// Lower it only for a store that takes smaller parts, such as a test's
	// fake: S3 refuses the completion of an upload with a smaller part.
	MinPartSize int64
	// MaxAttempts bounds each request's attempts (0: DefaultMaxAttempts),
	// the client's retry policy otherwise kept: what it deems transient
	// (5xx, 429, throttling, connection failures) is retried, and on
	// PutObject also 409 ConditionalRequestConflict.
	MaxAttempts int
	// AbortTimeout bounds an abort (0: DefaultAbortTimeout).
	AbortTimeout time.Duration
	// Retried, if set, is called with each failed attempt the client's
	// policy retries, and the operation it belongs to. It must return
	// quickly.
	Retried func(op string, err error)
}

// NewWriter returns a Writer of the objects of bucket through client,
// which carries the caller's credentials, region, endpoint and transport.
// CreateOnly is If-None-Match: * on the PutObject or the
// CompleteMultipartUpload that creates the object, which S3 checks when
// it creates it. Content is stored as given, typed
// application/octet-stream, with a CRC32 checksum the service verifies.
func NewWriter(client *s3.Client, bucket string, o WriteOptions) (objfile.Writer, error) {
	if o.MinPartSize <= 0 {
		o.MinPartSize = MinPartSize
	}
	if o.PartSize == 0 {
		o.PartSize = max(DefaultPartSize, o.MinPartSize)
	}
	if o.PartSize < o.MinPartSize || o.PartSize > MaxPartSize {
		return nil, fmt.Errorf("objfile/s3: part size %d is not within %d to %d bytes", o.PartSize, o.MinPartSize, int64(MaxPartSize))
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}
	if o.AbortTimeout <= 0 {
		o.AbortTimeout = DefaultAbortTimeout
	}
	w := &writer{client: client, bucket: bucket, o: o}
	w.pool.New = func() any { b := make([]byte, 0, o.PartSize); return &b }
	return w, nil
}

type writer struct {
	client *s3.Client
	bucket string
	o      WriteOptions
	pool   sync.Pool // *[]byte of PartSize capacity
}

// opts are a request's options: the attempt bound, retry accounting under
// op's name, and for PutObject a 409 retried (S3's answer to a concurrent
// conditional write; the request can be sent again whole).
func (w *writer) opts(op string) func(*s3.Options) {
	return func(o *s3.Options) {
		r := retry.AddWithMaxAttempts(o.Retryer, w.o.MaxAttempts)
		if op == "PutObject" {
			r = retry.AddWithErrorCodes(r, "ConditionalRequestConflict")
		}
		o.Retryer = counted{RetryerV2: r.(aws.RetryerV2), op: op, retried: w.o.Retried}
	}
}

// counted tells retried of each retry: the retry middleware asks for a
// retry token only once it has decided to retry.
type counted struct {
	aws.RetryerV2
	op      string
	retried func(string, error)
}

func (c counted) GetRetryToken(ctx context.Context, err error) (func(error) error, error) {
	release, terr := c.RetryerV2.GetRetryToken(ctx, err)
	if terr == nil && c.retried != nil {
		c.retried(c.op, err)
	}
	return release, terr
}

func (w *writer) NewWriter(ctx context.Context, name string, c objfile.Condition) (objfile.Upload, error) {
	if c != objfile.CreateOnly {
		return nil, fmt.Errorf("objfile/s3: write %s: unsupported condition %d", name, c)
	}
	ctx, cancel := context.WithCancel(ctx)
	return &upload{w: w, name: name, ctx: ctx, cancel: cancel}, nil
}

// upload is one object's upload. Write fills one buffer of PartSize and,
// when more bytes follow a full one, sends it as the next part before it
// returns: memory is one part an upload, and a slow store holds the
// writer back. Close sends what is left: the whole object by PutObject
// when it never filled more than a buffer, else the last part and the
// completion.
type upload struct {
	w      *writer
	name   string
	ctx    context.Context
	cancel context.CancelFunc

	aborted atomic.Bool

	mu      sync.Mutex // held by Write, Close and Abort while they work
	buf     *[]byte
	id      string // the multipart upload's, once created
	parts   []types.CompletedPart
	n       int64 // bytes written
	state   int   // 0 open, 1 closed, 2 failed
	receipt objfile.Receipt
}

func (u *upload) Write(p []byte) (int, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.usable(); err != nil {
		return 0, err
	}
	if u.buf == nil {
		u.buf = u.w.pool.Get().(*[]byte)
	}
	written := 0
	for len(p) > 0 {
		b := *u.buf
		if int64(len(b)) == u.w.o.PartSize {
			// A full buffer with more to come: it is a part, and the more
			// another, which must be within the limit.
			err := fmt.Errorf("the object exceeds %d parts of %d bytes", maxParts, u.w.o.PartSize)
			if len(u.parts)+2 <= maxParts {
				err = u.sendPart()
			}
			if err != nil {
				u.state = 2
				return written, u.fail(err)
			}
			b = *u.buf
		}
		k := min(len(p), int(u.w.o.PartSize)-len(b))
		*u.buf = append(b, p[:k]...)
		p, written, u.n = p[k:], written+k, u.n+int64(k)
	}
	return written, nil
}

func (u *upload) usable() error {
	switch {
	case u.aborted.Load() || u.state == 2:
		return u.pathError(errors.New("upload aborted or failed"))
	case u.state == 1:
		return u.pathError(errors.New("upload already closed"))
	}
	return nil
}

// sendPart sends the full buffer as the next part, starting the multipart
// upload first if it is the first.
func (u *upload) sendPart() error {
	if len(u.parts)+1 > maxParts {
		return fmt.Errorf("the object exceeds %d parts of %d bytes", maxParts, u.w.o.PartSize)
	}
	if u.id == "" {
		out, err := u.w.client.CreateMultipartUpload(u.ctx, &s3.CreateMultipartUploadInput{Bucket: &u.w.bucket, Key: &u.name,
			ContentType: aws.String("application/octet-stream"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}, u.w.opts("CreateMultipartUpload"))
		if err != nil {
			return err
		}
		if aws.ToString(out.UploadId) == "" {
			return errors.New("CreateMultipartUpload returned no upload id")
		}
		u.id = *out.UploadId
	}
	num := int32(len(u.parts) + 1)
	out, err := u.w.client.UploadPart(u.ctx, &s3.UploadPartInput{Bucket: &u.w.bucket, Key: &u.name, UploadId: &u.id, PartNumber: &num,
		Body: bytes.NewReader(*u.buf), ContentLength: aws.Int64(int64(len(*u.buf))), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}, u.w.opts("UploadPart"))
	if err != nil {
		return err
	}
	u.parts = append(u.parts, types.CompletedPart{PartNumber: &num, ETag: out.ETag, ChecksumCRC32: out.ChecksumCRC32})
	*u.buf = (*u.buf)[:0]
	return nil
}

func (u *upload) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.usable(); err != nil {
		return err
	}
	err := u.complete()
	u.release()
	u.cancel()
	if err != nil {
		u.state = 2
		return u.fail(err)
	}
	u.state = 1
	return nil
}

// complete creates the object: by PutObject when no part was sent, else
// the last part and CompleteMultipartUpload, both If-None-Match: *.
func (u *upload) complete() error {
	var data []byte
	if u.buf != nil {
		data = *u.buf
	}
	if u.id == "" {
		r, err := u.w.put(u.ctx, u.name, data)
		u.receipt = r
		return err
	}
	if len(data) > 0 {
		if err := u.sendPart(); err != nil {
			return err
		}
	}
	out, err := u.w.client.CompleteMultipartUpload(u.ctx, &s3.CompleteMultipartUploadInput{Bucket: &u.w.bucket, Key: &u.name, UploadId: &u.id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: u.parts}, IfNoneMatch: aws.String("*"), MpuObjectSize: aws.Int64(u.n)},
		u.w.opts("CompleteMultipartUpload"))
	if err != nil {
		return err
	}
	u.id = "" // completed: nothing to abort
	r, err := receiptOf(u.name, u.n, out.ETag, out.VersionId)
	u.receipt = r
	return err
}

// release returns the buffer to the pool.
func (u *upload) release() {
	if u.buf != nil {
		*u.buf = (*u.buf)[:0]
		u.w.pool.Put(u.buf)
		u.buf = nil
	}
}

// fail ends a failed upload: its multipart upload, if any, is aborted, and
// err is returned in objfile's terms with the upload id redacted.
func (u *upload) fail(err error) error {
	id := u.id
	u.abortUpload()
	return u.pathError(classify(redact.Error(err, id, "[upload-id]")))
}

// abortUpload aborts the multipart upload, if one was created, under a
// context of its own (the upload's may be cancelled). An abort that fails
// leaves the upload's parts stored until a lifecycle rule removes them.
func (u *upload) abortUpload() {
	if u.id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(u.ctx), u.w.o.AbortTimeout)
	defer cancel()
	_, _ = u.w.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &u.w.bucket, Key: &u.name, UploadId: &u.id}, u.w.opts("AbortMultipartUpload"))
	u.id = ""
}

// Abort cancels the upload: a request in flight is cancelled, then the
// multipart upload, if one was created, is aborted (its parts deleted)
// under a bounded context of its own. No object is created. Abort may be
// called from another goroutine than Write's: it waits only for the
// cancelled request to return.
func (u *upload) Abort() error {
	if u.aborted.Swap(true) {
		return nil
	}
	u.cancel()
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.state == 1 {
		return nil // completed: Abort does nothing
	}
	u.abortUpload()
	u.release()
	return nil
}

func (u *upload) Receipt() (objfile.Receipt, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.receipt, u.state == 1
}

func (u *upload) pathError(err error) error {
	return &fs.PathError{Op: "write", Path: u.name, Err: err}
}

// put creates name holding data by one PutObject, If-None-Match: *.
func (w *writer) put(ctx context.Context, name string, data []byte) (objfile.Receipt, error) {
	out, err := w.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &w.bucket, Key: &name, Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
		IfNoneMatch: aws.String("*"), ContentType: aws.String("application/octet-stream"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}, w.opts("PutObject"))
	if err != nil {
		return objfile.Receipt{}, err
	}
	return receiptOf(name, int64(len(data)), out.ETag, out.VersionId)
}

// receiptOf is the receipt of a created object: its Identity is the
// version id in a versioned bucket, else the ETag, as an Object's.
func receiptOf(name string, size int64, etag, version *string) (objfile.Receipt, error) {
	id := aws.ToString(version)
	if id == "" || id == "null" {
		id = aws.ToString(etag)
	}
	if id == "" {
		return objfile.Receipt{}, errors.New("the service returned no ETag")
	}
	return objfile.Receipt{Name: name, Size: size, Identity: id}, nil
}

func (w *writer) PutIfAbsent(ctx context.Context, name string, data []byte) (objfile.Receipt, error) {
	r, err := w.put(ctx, name, data)
	if err != nil {
		return objfile.Receipt{}, &fs.PathError{Op: "write", Path: name, Err: classify(err)}
	}
	return r, nil
}

// DeleteIf deletes the version identity names: a version id (in a
// versioned bucket) is deleted by its id, which removes that version
// only; an ETag (quoted, as S3 sends it) is sent as If-Match, which S3
// checks against the live object. A store that ignores If-Match on
// DeleteObject deletes the live object whatever it holds.
func (w *writer) DeleteIf(ctx context.Context, name, identity string) error {
	if identity == "" {
		return &fs.PathError{Op: "delete", Path: name, Err: fmt.Errorf("%w: no identity", fs.ErrInvalid)}
	}
	in := &s3.DeleteObjectInput{Bucket: &w.bucket, Key: &name}
	if strings.HasPrefix(identity, `"`) {
		in.IfMatch = &identity
	} else {
		in.VersionId = &identity
	}
	_, err := w.client.DeleteObject(ctx, in, w.opts("DeleteObject"))
	switch s := status(err); {
	case err == nil:
		return nil
	case s == http.StatusNotFound:
		err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
	case s == http.StatusPreconditionFailed:
		err = fmt.Errorf("%w: %w", objfile.ErrObjectChanged, err)
	}
	return &fs.PathError{Op: "delete", Path: name, Err: err}
}

// classify marks a failed write in objfile's terms: a failed precondition
// (412) is ErrExists; a 409 ConditionalRequestConflict (a conflicting
// conditional write, after PutObject's retries; at once for a
// completion, whose upload is then aborted) is ErrConflict.
func classify(err error) error {
	var ae smithy.APIError
	switch {
	case status(err) == http.StatusPreconditionFailed:
		return fmt.Errorf("%w: %w", objfile.ErrExists, err)
	case status(err) == http.StatusConflict, errors.As(err, &ae) && ae.ErrorCode() == "ConditionalRequestConflict":
		return fmt.Errorf("%w: %w", objfile.ErrConflict, err)
	}
	return err
}
