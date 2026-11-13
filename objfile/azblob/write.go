// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc64"
	"io/fs"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/redact"
)

// Block blob limits: a block (and the buffer, which is also a Put Blob's
// body) at most MaxBlockSize bytes, and at most MaxBlocks blocks a blob,
// as the SDK states them. The service has no smallest block.
const (
	MaxBlockSize = blockblob.MaxUploadBlobBytes
	MaxBlocks    = blockblob.MaxBlocks
)

// maxBlocks is MaxBlocks; a test lowers it.
var maxBlocks = MaxBlocks

// DefaultBlockSize is the bytes an upload buffers and stages as one block.
const DefaultBlockSize = 8 << 20

// DefaultMaxAttempts bounds each request's attempts, the first included.
const DefaultMaxAttempts = 5

// WriteOptions configure a Writer.
type WriteOptions struct {
	// BlockSize is an upload's buffer and the size of each block but the
	// last (0: DefaultBlockSize); 1 to MaxBlockSize. An object of
	// BlockSize bytes or fewer is created by one Put Blob; a larger one is
	// staged in blocks and committed, so the largest object is MaxBlocks ×
	// BlockSize.
	BlockSize int64
	// MaxAttempts bounds each request's attempts (0: DefaultMaxAttempts).
	// What azcore deems transient is retried: a transport failure, 408,
	// 429, 500, 502, 503 and 504. The writer's requests take their retry
	// policy from these options, not from the client's.
	MaxAttempts int
	// RetryDelay and MaxRetryDelay shape azcore's backoff (0: azcore's,
	// 800 ms and 60 s): the n-th retry waits about (2^n - 1) × RetryDelay,
	// with jitter, at most MaxRetryDelay. A Retry-After the service sends
	// is waited instead, unless it exceeds MaxRetryDelay: then the request
	// is not retried.
	RetryDelay, MaxRetryDelay time.Duration
	// Retried, if set, is called with each failed attempt that was retried,
	// and the operation it belongs to, once the attempt the retry made has
	// ended normally: a retry azcore declines (a Retry-After over
	// MaxRetryDelay) is not counted, nor is one whose attempt is cancelled
	// after it was sent or ends in an error azcore does not retry, so a run
	// cancelled mid-retry can under-report by one. It must return quickly.
	Retried func(op string, err error)
}

// NewWriter returns a Writer of the blobs of containerName through
// client, which carries the caller's credentials, endpoint and transport.
// Objects are block blobs. CreateOnly is If-None-Match: * on the Put Blob
// or the Put Block List that creates the blob, which the service checks
// when it creates it. Content is stored as given, typed
// application/octet-stream, each request's body with a CRC64 the service
// verifies.
// Each upload buffers one block and blocks Write while staging it. Abort
// cancels requests but sends no block deletion: uncommitted blocks expire
// a week after the last was staged, or are discarded when
// a blob is committed at the name. They require no lifecycle rule. Block ids
// are unique to an upload and are removed from request errors.
// Requests use the upload's context, without a separate block-transfer timeout.
//
// DeleteIf sends If-Match against the current blob's ETag. A service 404 wraps
// fs.ErrNotExist and a 412 wraps objfile.ErrObjectChanged. Soft-deleted blobs
// remain until the account's retention period expires; with versioning, deletion
// retains a previous version until explicitly deleted or removed by a lifecycle
// rule. Conditional deletion does not bypass these retention policies.
func NewWriter(client *azsdk.Client, containerName string, o WriteOptions) (objfile.Writer, error) {
	if o.BlockSize == 0 {
		o.BlockSize = DefaultBlockSize
	}
	if o.BlockSize < 1 || o.BlockSize > MaxBlockSize {
		return nil, fmt.Errorf("objfile/azblob: block size %d is not within 1 to %d bytes", o.BlockSize, int64(MaxBlockSize))
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}
	w := &writer{container: client.ServiceClient().NewContainerClient(containerName), o: o}
	w.pool.New = func() any { b := make([]byte, 0, o.BlockSize); return &b }
	return w, nil
}

type writer struct {
	container *container.Client
	o         WriteOptions
	pool      sync.Pool // *[]byte of BlockSize capacity
}

// retryStatus are the answers azcore retries by default.
var retryStatus = []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
	http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout}

// call is ctx carrying the retry policy of one request of op: at most
// MaxAttempts attempts, each retry told to Retried. azcore asks
// ShouldRetry once each attempt has ended, and may still decline a retry
// it was told to make (a Retry-After over MaxRetryDelay, a cancellation
// during the backoff), so a retry is told to Retried at the next call,
// when its own attempt has ended. uncertain, if set, is set when a retry
// is asked for: every failure retried may have reached the service, which
// a later refusal does not undo (completionError); a declined retry
// leaves that failure the request's own, unresolved either way.
func (w *writer) call(ctx context.Context, op string, uncertain *atomic.Bool) context.Context {
	tries := 0
	var pending error // the failure a retry was asked for, until its attempt ends
	return policy.WithRetryOptions(ctx, policy.RetryOptions{
		MaxRetries: int32(max(w.o.MaxAttempts-1, -1)), RetryDelay: w.o.RetryDelay, MaxRetryDelay: w.o.MaxRetryDelay,
		ShouldRetry: func(resp *http.Response, err error) bool {
			if pending != nil && w.o.Retried != nil {
				w.o.Retried(op, pending)
			}
			pending = nil
			if err == nil && !slices.Contains(retryStatus, resp.StatusCode) {
				return false
			}
			if tries++; tries >= w.o.MaxAttempts {
				return false
			}
			if pending = err; err == nil {
				pending = runtime.NewResponseError(resp)
			}
			if uncertain != nil {
				uncertain.Store(true)
			}
			return true
		},
	})
}

// NewWriter starts name's upload, CreateOnly; requests begin when more bytes
// follow the first full block, or at Close.
func (w *writer) NewWriter(ctx context.Context, name string, c objfile.Condition) (objfile.Upload, error) {
	if c != objfile.CreateOnly {
		return nil, fmt.Errorf("objfile/azblob: write %s: unsupported condition %d", name, c)
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	return &upload{w: w, name: name, blob: w.container.NewBlockBlobClient(name), nonce: hex.EncodeToString(nonce[:]), ctx: ctx, cancel: cancel}, nil
}

// upload is one object's upload. Write fills one buffer of BlockSize and,
// when more bytes follow a full one, stages it as the next block before it
// returns: memory is one block an upload, and a slow store holds the
// writer back. Close sends what is left: the whole object by Put Blob when
// it never filled more than a buffer, else the last block and the block
// list, which creates the blob.
type upload struct {
	w      *writer
	name   string
	blob   *blockblob.Client
	nonce  string // the upload's own part of its block ids (blockID)
	ctx    context.Context
	cancel context.CancelFunc

	aborted atomic.Bool

	mu      sync.Mutex // held by Write, Close and Abort while they work
	buf     *[]byte
	blocks  []string // the ids of the blocks staged, in order
	n       int64    // bytes written
	state   int      // 0 open, 1 closed, 2 failed
	receipt objfile.Receipt

	uncertain atomic.Bool // the creating request was retried after an ambiguous failure
}

// blockID is the id of the upload's block i: its nonce and i, fixed-width
// (a blob's block ids must all have one length), Base64. The nonce keeps
// two uploads of one name apart: a block staged under an id another upload
// also uses would replace that upload's block.
func (u *upload) blockID(i int) string {
	return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s%06d", u.nonce, i))
}

// Write buffers p, staging each full buffer as a block once more bytes
// follow it. A failed block fails the upload.
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
		if int64(len(b)) == u.w.o.BlockSize {
			// A full buffer with more to come: it is a block, and the more
			// another, which must be within the limit.
			err := fmt.Errorf("the object exceeds %d blocks of %d bytes", maxBlocks, u.w.o.BlockSize)
			if len(u.blocks)+2 <= maxBlocks {
				err = u.stage()
			}
			if err != nil {
				u.state = 2
				return written, u.fail(err)
			}
			b = *u.buf
		}
		k := min(len(p), int(u.w.o.BlockSize)-len(b))
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

// crcTable is Azure Storage's CRC64 (polynomial 0x9A6C9329AC4BC9B5).
var crcTable = crc64.MakeTable(0x9A6C9329AC4BC9B5)

// checksum has the SDK send data's CRC64 with it (x-ms-content-crc64),
// which the service verifies. It is computed here, over the buffer: the
// SDK's own computation copies the body first.
func checksum(data []byte) blob.TransferValidationType {
	return blob.TransferValidationTypeCRC64(crc64.Checksum(data, crcTable))
}

// stage sends the full buffer as the next block (Put Block, idempotent:
// a retry stages the same bytes under the same id).
func (u *upload) stage() error {
	if len(u.blocks)+1 > maxBlocks {
		return fmt.Errorf("the object exceeds %d blocks of %d bytes", maxBlocks, u.w.o.BlockSize)
	}
	id := u.blockID(len(u.blocks))
	_, err := u.blob.StageBlock(u.w.call(u.ctx, "PutBlock", nil), id, streaming.NopCloser(bytes.NewReader(*u.buf)),
		&blockblob.StageBlockOptions{TransactionalValidation: checksum(*u.buf)})
	if err != nil {
		return err
	}
	u.blocks = append(u.blocks, id)
	*u.buf = (*u.buf)[:0]
	return nil
}

// Close creates the object; the failure is objfile.ErrUnresolved when the
// creating request's outcome is unknown. Blocks left uncommitted are
// reclaimed by the service (Abort).
func (u *upload) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.usable(); err != nil {
		return err
	}
	completion, err := u.complete()
	u.release()
	u.cancel()
	if err != nil {
		u.state = 2
		if completion {
			err = completionError(err, u.uncertain.Load())
		}
		return u.fail(err)
	}
	u.state = 1
	return nil
}

// complete creates the object: by Put Blob when no block was staged, else
// the last block and Put Block List, both If-None-Match: *. completion
// says the error, if any, is the creating request's (whose failure may
// leave an object), not an earlier block's.
func (u *upload) complete() (completion bool, err error) {
	var data []byte
	if u.buf != nil {
		data = *u.buf
	}
	if len(u.blocks) == 0 {
		r, err := u.w.put(u.ctx, u.name, data, &u.uncertain)
		u.receipt = r
		return true, err
	}
	if len(data) > 0 {
		if err := u.stage(); err != nil {
			return false, err
		}
	}
	out, err := u.blob.CommitBlockList(u.w.call(u.ctx, "PutBlockList", &u.uncertain), u.blocks,
		&blockblob.CommitBlockListOptions{HTTPHeaders: octetStream, AccessConditions: createOnly})
	if err != nil {
		return true, err
	}
	r, err := receiptOf(u.name, u.n, out.ETag)
	u.receipt = r
	if err != nil {
		err = fmt.Errorf("%w: %w", objfile.ErrUnresolved, err) // created, but not as the answer says
	}
	return true, err
}

// release returns the buffer to the pool.
func (u *upload) release() {
	if u.buf != nil {
		*u.buf = (*u.buf)[:0]
		u.w.pool.Put(u.buf)
		u.buf = nil
	}
}

// fail returns a failed upload's err in objfile's terms, redacted
// (redact).
func (u *upload) fail(err error) error {
	return u.redact(u.pathError(classify(err)))
}

// redact keeps the upload's block ids out of err's text: every URL in it
// loses its query (redact.URLs), and the ids' common Base64 prefix (the
// nonce's), raw or URL-escaped, and the nonce itself are replaced wherever
// else they appear. errors.Is and errors.As still see err.
func (u *upload) redact(err error) error {
	err = redact.URLs(err)
	prefix := base64.StdEncoding.EncodeToString([]byte(u.nonce))
	for _, form := range []string{prefix, url.QueryEscape(prefix), url.PathEscape(prefix), u.nonce} {
		err = redact.Error(err, form, "[block-id]")
	}
	return err
}

// Abort cancels the upload. Before Close, no blob is created; after Close
// sends its creating request, Abort cannot undo creation or settle an
// unresolved outcome. No call is needed for blocks still uncommitted: they are never
// part of a blob or visible as one, and the service discards them when a
// blob is created at the name, or a week after the last was staged. Abort
// may be called from another goroutine than Write's: it waits only for
// the cancelled request to return. Abort after a successful Close does
// nothing. It returns nil.
func (u *upload) Abort() error {
	u.aborted.Store(true)
	u.cancel()
	u.mu.Lock()
	defer u.mu.Unlock()
	u.release()
	return nil
}

// Receipt is the created object's, once Close has succeeded.
func (u *upload) Receipt() (objfile.Receipt, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.receipt, u.state == 1
}

func (u *upload) pathError(err error) error {
	return &fs.PathError{Op: "write", Path: u.name, Err: err}
}

var (
	octetStream = &blob.HTTPHeaders{BlobContentType: to.Ptr("application/octet-stream")}
	anyETag     = azcore.ETagAny
	createOnly  = &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: &anyETag}}
)

// put creates name holding data by one Put Blob, If-None-Match: *;
// uncertain is marked as call marks it.
func (w *writer) put(ctx context.Context, name string, data []byte, uncertain *atomic.Bool) (objfile.Receipt, error) {
	out, err := w.container.NewBlockBlobClient(name).Upload(w.call(ctx, "PutBlob", uncertain), streaming.NopCloser(bytes.NewReader(data)),
		&blockblob.UploadOptions{HTTPHeaders: octetStream, AccessConditions: createOnly, TransactionalValidation: checksum(data)})
	if err != nil {
		return objfile.Receipt{}, err
	}
	return receiptOf(name, int64(len(data)), out.ETag)
}

// receiptOf is the receipt of a created blob: its Identity is the ETag,
// as an Object's.
func receiptOf(name string, size int64, etag *azcore.ETag) (objfile.Receipt, error) {
	if deref(etag) == "" {
		return objfile.Receipt{}, errors.New("the service returned no ETag")
	}
	return objfile.Receipt{Name: name, Size: size, Identity: string(*etag)}, nil
}

// PutIfAbsent creates name holding data by one Put Blob, If-None-Match: *.
func (w *writer) PutIfAbsent(ctx context.Context, name string, data []byte) (objfile.Receipt, error) {
	var uncertain atomic.Bool
	r, err := w.put(ctx, name, data, &uncertain)
	if err != nil {
		return objfile.Receipt{}, redact.URLs(&fs.PathError{Op: "write", Path: name, Err: classify(completionError(err, uncertain.Load()))})
	}
	return r, nil
}

// DeleteIf deletes the current blob under If-Match on identity.
func (w *writer) DeleteIf(ctx context.Context, name, identity string) error {
	if identity == "" {
		return &fs.PathError{Op: "delete", Path: name, Err: fmt.Errorf("%w: no identity", fs.ErrInvalid)}
	}
	etag := azcore.ETag(identity)
	_, err := w.container.NewBlobClient(name).Delete(w.call(ctx, "DeleteBlob", nil),
		&blob.DeleteOptions{AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &etag}}})
	switch s := status(err); {
	case err == nil:
		return nil
	case s == http.StatusNotFound:
		err = fmt.Errorf("%w: %w", fs.ErrNotExist, err)
	case s == http.StatusPreconditionFailed:
		err = fmt.Errorf("%w: %w", objfile.ErrObjectChanged, err)
	}
	return redact.URLs(&fs.PathError{Op: "delete", Path: name, Err: err})
}

// completionError marks a failed creating request (Put Blob, or Put Block
// List) whose blob may exist, or may yet, as objfile.ErrUnresolved: a
// transport failure, a timeout, a cancellation, a server error, 408 or
// 429. A refusal (another 4xx) leaves none, unless an earlier attempt of
// the request failed ambiguously (uncertain: it was retried), which the
// refusal does not undo; a blob found at the name stays classify's (a
// retry that finds the blob, the request's own or another's, is how a lost
// answer looks too).
func completionError(err error, uncertain bool) error {
	code := status(err)
	switch {
	case errors.Is(err, objfile.ErrUnresolved), exists(err):
		return err
	case uncertain:
	case code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests:
		return err
	}
	return fmt.Errorf("%w: %w", objfile.ErrUnresolved, err)
}

// classify marks a failed write in objfile's terms: a blob found at the
// name is ErrExists. The service serializes conditional writes of a
// name, so no answer is ErrConflict; a 409 other than BlobAlreadyExists
// (a lease, an immutability policy) is a refusal.
func classify(err error) error {
	if exists(err) {
		return fmt.Errorf("%w: %w", objfile.ErrExists, err)
	}
	return err
}

// exists reports the service's answers to If-None-Match: * at a name that
// holds a blob: 409 BlobAlreadyExists, or 412.
func exists(err error) bool {
	var re *azcore.ResponseError
	return errors.As(err, &re) && (re.StatusCode == http.StatusPreconditionFailed ||
		re.StatusCode == http.StatusConflict && re.ErrorCode == "BlobAlreadyExists")
}
