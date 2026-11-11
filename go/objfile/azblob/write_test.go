// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/azblob/azfake"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

// block is the tests' block size: a staged upload takes kilobytes.
const block = 64 << 10

type fixture struct {
	f       *azfake.Server
	w       objfile.Writer
	o       objfile.Opener
	retries *atomic.Int64
	ops     *sync.Map // retries by operation
	ids     *sync.Map // block ids staged, by request
}

func newFixture(t *testing.T, wrap func(http.Handler) http.Handler, o azblob.WriteOptions) fixture {
	t.Helper()
	var ids sync.Map
	f, c := fakeClient(t, func(h http.Handler) http.Handler {
		if wrap != nil {
			h = wrap(h)
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if id := r.URL.Query().Get("blockid"); id != "" {
				ids.Store(id, true)
			}
			h.ServeHTTP(w, r)
		})
	})
	var retries atomic.Int64
	var ops sync.Map
	if o.BlockSize == 0 {
		o.BlockSize = block
	}
	o.RetryDelay, o.MaxRetryDelay = time.Millisecond, time.Millisecond
	o.Retried = func(op string, _ error) {
		retries.Add(1)
		n, _ := ops.LoadOrStore(op, new(atomic.Int64))
		n.(*atomic.Int64).Add(1)
	}
	w, err := azblob.NewWriter(c, containerName, o)
	if err != nil {
		t.Fatal(err)
	}
	return fixture{f: f, w: w, o: azblob.New(c, containerName), retries: &retries, ops: &ops, ids: &ids}
}

func (x fixture) retried(op string) int64 {
	n, ok := x.ops.Load(op)
	if !ok {
		return 0
	}
	return n.(*atomic.Int64).Load()
}

// leaks says whether text shows a block id the fixture saw staged, or any
// part of one an upload shares with its others.
func (x fixture) leaks(text string) bool {
	found := strings.Contains(text, "blockid=")
	x.ids.Range(func(k, _ any) bool {
		id := k.(string)
		raw, _ := base64.StdEncoding.DecodeString(id)
		for _, form := range []string{id, id[:32], string(raw[:24])} {
			if strings.Contains(text, form) {
				found = true
			}
		}
		return !found
	})
	return found
}

func upload(t *testing.T, w objfile.Writer, name string, data []byte) (objfile.Receipt, error) {
	t.Helper()
	u, err := w.NewWriter(context.Background(), name, objfile.CreateOnly)
	if err != nil {
		t.Fatal(err)
	}
	// In pieces, as the part writer writes (4 KiB at a time).
	for off := 0; off < len(data); off += 4096 {
		if _, err := u.Write(data[off:min(off+4096, len(data))]); err != nil {
			_ = u.Abort()
			return objfile.Receipt{}, err
		}
	}
	if err := u.Close(); err != nil {
		_ = u.Abort()
		return objfile.Receipt{}, err
	}
	r, ok := u.Receipt()
	if !ok {
		t.Fatal("no receipt after a successful Close")
	}
	return r, nil
}

func iff(c bool, status int) int {
	if c {
		return status
	}
	return 0
}

// TestUpload: an object of at most a block is one Put Blob, a larger one
// blocks staged and a block list committed; each is created whole, with a
// receipt whose identity is what a read pins (the ETag), and no block is
// left uncommitted.
func TestUpload(t *testing.T) {
	for _, n := range []int{0, 1000, block, block + 1, 3*block + 17} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			x := newFixture(t, nil, azblob.WriteOptions{})
			data := objfiletest.Data(n, 3)
			r, err := upload(t, x.w, "d/obj", data)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := x.f.Get("d/obj")
			if !ok || !bytes.Equal(got.Data, data) {
				t.Fatalf("stored %d bytes, want %d", len(got.Data), n)
			}
			if r.Name != "d/obj" || r.Size != int64(n) || r.Identity != got.ETag {
				t.Fatalf("receipt %+v, blob %s", r, got.ETag)
			}
			blocks := int64(0)
			if n > block {
				blocks = int64((n + block - 1) / block)
			}
			if x.f.Count(azfake.OpChunk) != blocks || x.f.Count(azfake.OpCreate) != 1 {
				t.Fatalf("%d bytes: %d blocks staged, %d creations", n, x.f.Count(azfake.OpChunk), x.f.Count(azfake.OpCreate))
			}
			if len(x.f.Staging()) != 0 {
				t.Fatalf("blocks left uncommitted under %v", x.f.Staging())
			}
			if n == 0 {
				return
			}
			obj, err := x.o.Open(context.Background(), "d/obj")
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()
			if obj.Identity() != r.Identity {
				t.Fatalf("read pins %s, receipt %s", obj.Identity(), r.Identity)
			}
		})
	}
}

// TestBlockLimits: the block size is refused outside the service's
// limits, and an object of more blocks than the limit fails at the block
// past it, creating nothing.
func TestBlockLimits(t *testing.T) {
	_, c := fakeClient(t, nil)
	for _, o := range []azblob.WriteOptions{{BlockSize: -1}, {BlockSize: azblob.MaxBlockSize + 1}} {
		if _, err := azblob.NewWriter(c, containerName, o); err == nil {
			t.Errorf("%+v: accepted", o)
		}
	}
	if w, err := azblob.NewWriter(c, containerName, azblob.WriteOptions{}); err != nil || w == nil {
		t.Fatalf("defaults: %v", err)
	}

	defer azblob.SetMaxBlocks(3)()
	x := newFixture(t, nil, azblob.WriteOptions{})
	if _, err := upload(t, x.w, "three", objfiletest.Data(3*block, 1)); err != nil {
		t.Fatalf("3 blocks: %v", err)
	}
	if _, err := upload(t, x.w, "four", objfiletest.Data(3*block+1, 1)); err == nil || !strings.Contains(err.Error(), "exceeds 3 blocks") {
		t.Fatalf("4 blocks: %v", err)
	}
	if _, ok := x.f.Get("four"); ok || x.f.Count(azfake.OpChunk) != 3+2 {
		t.Fatalf("the refused object exists, or a block past the limit was staged: %d staged", x.f.Count(azfake.OpChunk))
	}
}

// TestBlockIDs: an upload's block ids have one length, follow its blocks'
// order, and are its own: two uploads of one name stage side by side
// without replacing each other's blocks, the first to commit creates the
// blob with its own bytes, and the other fails with ErrExists.
func TestBlockIDs(t *testing.T) {
	x := newFixture(t, nil, azblob.WriteOptions{})
	a, _ := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
	b, _ := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
	da, db := objfiletest.Data(3*block, 1), objfiletest.Data(3*block, 2)
	for off := 0; off < len(da); off += block / 2 {
		if _, err := a.Write(da[off : off+block/2]); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Write(db[off : off+block/2]); err != nil {
			t.Fatal(err)
		}
	}
	if n := x.f.Uncommitted("obj"); n != 4 {
		t.Fatalf("%d blocks staged, want 2 of each upload", n)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); !errors.Is(err, objfile.ErrExists) {
		t.Fatalf("the second commit: %v, want ErrExists", err)
	}
	if o, _ := x.f.Get("obj"); !bytes.Equal(o.Data, da) {
		t.Fatal("the blob is not the first upload's bytes")
	}
	width := -1
	x.ids.Range(func(k, _ any) bool {
		if width >= 0 && len(k.(string)) != width {
			t.Errorf("block ids of %d and %d characters", width, len(k.(string)))
		}
		width = len(k.(string))
		return true
	})
}

// TestUncommittedInvisible: staged blocks are no blob: while an upload
// stages, and after it is aborted, the name cannot be opened or read and
// is not among the blobs, and Abort sends nothing. A later upload of the
// name creates its own blob, and its creation discards the blocks left.
func TestUncommittedInvisible(t *testing.T) {
	x := newFixture(t, nil, azblob.WriteOptions{})
	u, _ := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
	if _, err := u.Write(objfiletest.Data(3*block+1, 1)); err != nil {
		t.Fatal(err)
	}
	invisible := func(when string) {
		t.Helper()
		if x.f.Uncommitted("obj") != 3 {
			t.Fatalf("%s: %d blocks staged, want 3", when, x.f.Uncommitted("obj"))
		}
		if _, err := x.o.Open(context.Background(), "obj"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s: open: %v, want fs.ErrNotExist", when, err)
		}
		obj, _ := x.o.OpenSized(context.Background(), "obj", 10)
		if _, err := obj.ReadAt(make([]byte, 10), 0); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s: read: %v, want fs.ErrNotExist", when, err)
		}
		if len(x.f.Names()) != 0 {
			t.Fatalf("%s: blobs %v", when, x.f.Names())
		}
	}
	invisible("staging")
	requests := x.f.Count(azfake.OpChunk) + x.f.Count(azfake.OpCreate) + x.f.Count(azfake.OpDelete)
	if err := u.Abort(); err != nil {
		t.Fatal(err)
	}
	if x.f.Count(azfake.OpChunk)+x.f.Count(azfake.OpCreate)+x.f.Count(azfake.OpDelete) != requests {
		t.Fatal("Abort sent a request")
	}
	invisible("aborted")
	data := objfiletest.Data(2*block+5, 7)
	if _, err := upload(t, x.w, "obj", data); err != nil {
		t.Fatal(err)
	}
	if o, _ := x.f.Get("obj"); !bytes.Equal(o.Data, data) || x.f.Uncommitted("obj") != 0 {
		t.Fatalf("the later upload's blob, or the blocks left (%d)", x.f.Uncommitted("obj"))
	}
}

// TestCreateOnly: a blob already at the name, or created there while an
// upload stages, fails the upload with ErrExists and is left as it was;
// so does a 412, the other answer of a failed If-None-Match. A 409 other
// than BlobAlreadyExists is a refusal: neither ErrExists nor ErrConflict.
func TestCreateOnly(t *testing.T) {
	for _, n := range []int{100, 3 * block} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			x := newFixture(t, nil, azblob.WriteOptions{})
			theirs := x.f.Put("obj", []byte("theirs"))
			if _, err := upload(t, x.w, "obj", objfiletest.Data(n, 1)); !errors.Is(err, objfile.ErrExists) || errors.Is(err, objfile.ErrConflict) {
				t.Fatalf("upload over a blob: %v, want ErrExists", err)
			}
			if o, _ := x.f.Get("obj"); o.ETag != theirs.ETag {
				t.Fatal("the existing blob changed")
			}
		})
	}
	t.Run("race", func(t *testing.T) {
		x := newFixture(t, nil, azblob.WriteOptions{})
		var once atomic.Bool
		x.f.SetFault(func(op, name string, after bool) int {
			if op == azfake.OpChunk && after && once.CompareAndSwap(false, true) {
				go x.f.Put(name, []byte("theirs")) // the fake's lock is held: put after this answer
			}
			return 0
		})
		if _, err := upload(t, x.w, "obj", objfiletest.Data(3*block, 1)); !errors.Is(err, objfile.ErrExists) {
			t.Fatalf("upload that lost a race: %v, want ErrExists", err)
		}
		if o, _ := x.f.Get("obj"); string(o.Data) != "theirs" {
			t.Fatal("the racing blob was replaced")
		}
	})
	t.Run("412", func(t *testing.T) {
		x := newFixture(t, nil, azblob.WriteOptions{})
		x.f.SetFault(func(op, _ string, after bool) int {
			return iff(op == azfake.OpCreate && !after, http.StatusPreconditionFailed)
		})
		if _, err := x.w.PutIfAbsent(context.Background(), "m", []byte("x")); !errors.Is(err, objfile.ErrExists) {
			t.Fatalf("412: %v, want ErrExists", err)
		}
	})
	t.Run("lease", func(t *testing.T) {
		x := newFixture(t, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("comp") == "blocklist" {
					w.Header().Set("x-ms-error-code", "LeaseIdMissing")
					w.WriteHeader(http.StatusConflict)
					return
				}
				h.ServeHTTP(w, r)
			})
		}, azblob.WriteOptions{})
		_, err := upload(t, x.w, "obj", objfiletest.Data(3*block, 1))
		if err == nil || errors.Is(err, objfile.ErrExists) || errors.Is(err, objfile.ErrConflict) || errors.Is(err, objfile.ErrUnresolved) {
			t.Fatalf("a 409 LeaseIdMissing: %v, want a refusal", err)
		}
	})
}

// TestTransient: a request the service fails transiently (503) is sent
// again and counted; one that keeps failing ends the upload after
// MaxAttempts, without a blob.
func TestTransient(t *testing.T) {
	for _, op := range []string{azfake.OpChunk, azfake.OpCreate} {
		for _, n := range []int{100, 3*block + 5} {
			t.Run(op+"/"+strconv.Itoa(n), func(t *testing.T) {
				x := newFixture(t, nil, azblob.WriteOptions{})
				var k atomic.Int32
				x.f.SetFault(func(o, _ string, after bool) int {
					return iff(o == op && !after && k.Add(1) == 1, http.StatusServiceUnavailable)
				})
				data := objfiletest.Data(n, 4)
				if _, err := upload(t, x.w, "obj", data); err != nil {
					t.Fatal(err)
				}
				if o, _ := x.f.Get("obj"); !bytes.Equal(o.Data, data) {
					t.Fatal("the blob is not the bytes written")
				}
				if want := int64(iff(k.Load() > 0, 1)); x.retries.Load() != want {
					t.Fatalf("%d retries counted, want %d", x.retries.Load(), want)
				}
			})
		}
	}
	// A Retry-After over MaxRetryDelay: azcore does not retry, and no retry
	// is counted; the server error leaves the creation unresolved.
	t.Run("retry-after", func(t *testing.T) {
		var requests atomic.Int32
		x := newFixture(t, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Retry-After", "120")
				w.Header().Set("x-ms-error-code", "ServerBusy")
				w.WriteHeader(http.StatusServiceUnavailable)
			})
		}, azblob.WriteOptions{})
		_, err := x.w.PutIfAbsent(context.Background(), "m", []byte("x"))
		if !errors.Is(err, objfile.ErrUnresolved) || requests.Load() != 1 || x.retries.Load() != 0 {
			t.Fatalf("%v; %d requests, %d retries counted; want an unresolved failure, 1 and 0", err, requests.Load(), x.retries.Load())
		}
	})
	t.Run("bounded", func(t *testing.T) {
		x := newFixture(t, nil, azblob.WriteOptions{MaxAttempts: 3})
		x.f.SetFault(func(op, _ string, after bool) int {
			return iff(op == azfake.OpChunk && !after, http.StatusServiceUnavailable)
		})
		if _, err := upload(t, x.w, "obj", objfiletest.Data(3*block, 4)); err == nil {
			t.Fatal("an upload whose blocks always fail succeeded")
		}
		if got := x.f.Count(azfake.OpChunk); got != 3 || x.retried("PutBlock") != 2 {
			t.Fatalf("%d block attempts, %d retries counted; want 3 and 2", got, x.retried("PutBlock"))
		}
		if _, ok := x.f.Get("obj"); ok {
			t.Fatal("a failed upload created its blob")
		}
	})
}

// TestCompletionUnresolved: a creation whose outcome is unknown is
// objfile.ErrUnresolved, never a clean failure: a transport failure, a
// server error that persists, a refusal (403) after an attempt that failed
// ambiguously (503, retried). Its answer lost after it took effect, the
// retry finds the blob: ErrExists, and the blob is there. A refusal alone
// is a clean failure, and leaves no blob.
func TestCompletionUnresolved(t *testing.T) {
	for _, c := range []struct {
		name  string
		size  int
		fault func(after bool, n int32) int // n: the creation's attempt
		blob  bool                          // the blob exists afterwards
		want  error                         // nil: a clean failure
	}{
		{"lost answer", 3 * block, func(after bool, n int32) int { return iff(after && n == 1, http.StatusServiceUnavailable) }, true, objfile.ErrExists},
		{"put lost answer", 100, func(after bool, n int32) int { return iff(after && n == 1, http.StatusServiceUnavailable) }, true, objfile.ErrExists},
		{"server error", 3 * block, func(after bool, _ int32) int { return iff(!after, http.StatusInternalServerError) }, false, objfile.ErrUnresolved},
		{"put server error", 100, func(after bool, _ int32) int { return iff(!after, http.StatusServiceUnavailable) }, false, objfile.ErrUnresolved},
		{"refused", 3 * block, func(after bool, _ int32) int { return iff(!after, http.StatusForbidden) }, false, nil},
		{"put refused", 100, func(after bool, _ int32) int { return iff(!after, http.StatusForbidden) }, false, nil},
		{"refused after 503", 3 * block, func(after bool, n int32) int {
			return iff(!after && n == 1, http.StatusServiceUnavailable) + iff(!after && n > 1, http.StatusForbidden)
		}, false, objfile.ErrUnresolved},
	} {
		t.Run(c.name, func(t *testing.T) {
			x := newFixture(t, nil, azblob.WriteOptions{})
			var n atomic.Int32
			x.f.SetFault(func(op, _ string, after bool) int {
				if op != azfake.OpCreate {
					return 0
				}
				if !after {
					return c.fault(after, n.Add(1))
				}
				return c.fault(after, n.Load())
			})
			_, err := upload(t, x.w, "obj", objfiletest.Data(c.size, 1))
			if err == nil {
				t.Fatal("succeeded")
			}
			if c.want != nil && !errors.Is(err, c.want) || c.want == nil && (errors.Is(err, objfile.ErrUnresolved) || errors.Is(err, objfile.ErrExists)) {
				t.Fatalf("%v, want %v", err, c.want)
			}
			if _, ok := x.f.Get("obj"); ok != c.blob {
				t.Fatalf("blob exists: %v, want %v", ok, c.blob)
			}
		})
	}
	t.Run("transport", func(t *testing.T) {
		x := newFixture(t, cut(func(r *http.Request) bool { return r.URL.Query().Get("comp") == "blocklist" }), azblob.WriteOptions{MaxAttempts: 2})
		if _, err := upload(t, x.w, "obj", objfiletest.Data(3*block, 1)); !errors.Is(err, objfile.ErrUnresolved) {
			t.Fatalf("a commit cut in transport: %v, want ErrUnresolved", err)
		}
	})
}

// cut is a handler wrapper that drops the connection of each request
// match selects.
func cut(match func(r *http.Request) bool) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if match(r) {
				conn, _, _ := w.(http.Hijacker).Hijack()
				_ = conn.Close()
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}

// TestBlockIDRedacted: a transport failure names the request's URL, which
// holds the block id; neither the upload's error nor its rendering shows
// a block id, or the part its ids share, and the error still tells a
// connection failure. So for a block and for the commit (then
// unresolved), and for a body that quotes the id back.
func TestBlockIDRedacted(t *testing.T) {
	for _, c := range []struct {
		name string
		wrap func(http.Handler) http.Handler
	}{
		{"block", cut(func(r *http.Request) bool {
			return r.URL.Query().Get("comp") == "block" && len(r.URL.Query().Get("blockid")) > 0
		})},
		{"commit", cut(func(r *http.Request) bool { return r.URL.Query().Get("comp") == "blocklist" })},
		{"echoed", func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if id := r.URL.Query().Get("blockid"); id != "" {
					w.Header().Set("x-ms-error-code", "InvalidQueryParameterValue")
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprintf(w, "<Error><Code>InvalidQueryParameterValue</Code><QueryParameterName>blockid</QueryParameterName><QueryParameterValue>%s</QueryParameterValue></Error>", id)
					return
				}
				h.ServeHTTP(w, r)
			})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			x := newFixture(t, c.wrap, azblob.WriteOptions{MaxAttempts: 2})
			_, err := upload(t, x.w, "obj", objfiletest.Data(3*block, 1))
			if err == nil {
				t.Fatal("succeeded")
			}
			for _, text := range []string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err.Error())} {
				if x.leaks(text) {
					t.Fatalf("a block id is shown: %s", text)
				}
			}
			if c.name == "echoed" {
				return
			}
			var ne net.Error
			if !errors.As(err, &ne) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !strings.Contains(err.Error(), "EOF") {
				t.Fatalf("the connection failure's class is lost: %v", err)
			}
		})
	}
}

// TestSASRedacted: a client authorized by a SAS in its URL sends it on
// every request; no error of a writer through it shows the signature,
// whether the service refuses the request or its connection is cut.
func TestSASRedacted(t *testing.T) {
	const sig = "SYNTHETICSIGNATURE0123456789"
	var refuse atomic.Bool
	f := azfake.New(account, containerName)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("sig") != sig {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case refuse.Load():
			w.Header().Set("x-ms-error-code", "AuthorizationPermissionMismatch")
			w.WriteHeader(http.StatusForbidden)
		case r.URL.Query().Get("comp") == "blocklist":
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
		default:
			f.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	c, err := azsdk.NewClientWithNoCredential(srv.URL+"/"+account+"/?sv=2026-06-06&sp=rcwd&sig="+sig, nil)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := azblob.NewWriter(c, containerName, azblob.WriteOptions{BlockSize: block, MaxAttempts: 1})
	var errs []error
	_, err = upload(t, w, "big", objfiletest.Data(3*block, 1)) // its commit cut
	errs = append(errs, err)
	refuse.Store(true)
	_, err = w.PutIfAbsent(context.Background(), "m", []byte("x"))
	errs = append(errs, err)
	errs = append(errs, w.DeleteIf(context.Background(), "m", `"0x1"`))
	for _, err := range errs {
		if err == nil || strings.Contains(fmt.Sprintf("%+v", err), sig) || strings.Contains(err.Error(), "sig=") {
			t.Fatalf("an error shows the signature, or there is none: %v", err)
		}
	}
	if !errors.Is(errs[0], objfile.ErrUnresolved) || !strings.Contains(errs[1].Error(), "403") {
		t.Fatalf("the errors' classes are lost: %v; %v", errs[0], errs[1])
	}
}

// TestAbort: an aborted upload creates nothing, before a byte, in its
// buffer or after blocks were staged, and its Close fails; Abort after a
// successful Close does nothing.
func TestAbort(t *testing.T) {
	for _, n := range []int{0, 100, 3 * block} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			x := newFixture(t, nil, azblob.WriteOptions{})
			u, err := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := u.Write(objfiletest.Data(n, 2)); err != nil {
				t.Fatal(err)
			}
			if err := u.Abort(); err != nil {
				t.Fatal(err)
			}
			if err := u.Close(); err == nil {
				t.Fatal("Close after Abort succeeded")
			}
			if _, ok := x.f.Get("obj"); ok || x.f.Count(azfake.OpCreate) != 0 {
				t.Fatal("an aborted upload created its blob")
			}
			if _, ok := u.Receipt(); ok {
				t.Fatal("an aborted upload has a receipt")
			}
		})
	}
	t.Run("after close", func(t *testing.T) {
		x := newFixture(t, nil, azblob.WriteOptions{})
		u, _ := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
		_, _ = u.Write(objfiletest.Data(3*block, 2))
		if err := u.Close(); err != nil {
			t.Fatal(err)
		}
		if err := u.Abort(); err != nil {
			t.Fatal(err)
		}
		if _, ok := x.f.Get("obj"); !ok {
			t.Fatal("Abort after Close removed the blob")
		}
		if _, ok := u.Receipt(); !ok {
			t.Fatal("no receipt")
		}
	})
	// The run is cancelled while a block is in flight.
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var blocks atomic.Int32
		x := newFixture(t, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("comp") == "block" && blocks.Add(1) == 2 {
					_, _ = io.Copy(io.Discard, r.Body) // the server notices a closed connection only after the body
					cancel()
					select {
					case <-r.Context().Done():
					case <-time.After(10 * time.Second):
					}
					return
				}
				h.ServeHTTP(w, r)
			})
		}, azblob.WriteOptions{})
		u, err := x.w.NewWriter(ctx, "obj", objfile.CreateOnly)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = u.Write(objfiletest.Data(3*block, 2)); !errors.Is(err, context.Canceled) {
			t.Fatalf("write cancelled mid-block: %v", err)
		}
		if err := u.Close(); err == nil {
			t.Fatal("Close of a cancelled upload succeeded")
		}
		if _, ok := x.f.Get("obj"); ok {
			t.Fatal("blob made")
		}
	})
	// Abort from another goroutine while Write stages blocks (go test -race).
	t.Run("concurrent", func(t *testing.T) {
		for range 20 {
			x := newFixture(t, nil, azblob.WriteOptions{})
			u, _ := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range 8 {
					if _, err := u.Write(objfiletest.Data(block/2, 1)); err != nil {
						return
					}
				}
			}()
			time.Sleep(time.Millisecond)
			_ = u.Abort()
			<-done
			if err := u.Close(); err == nil {
				t.Fatal("Close after a concurrent Abort succeeded")
			}
			if _, ok := x.f.Get("obj"); ok {
				t.Fatal("blob made")
			}
		}
	})
}

// TestPutIfAbsent: one request; a blob already there is ErrExists.
func TestPutIfAbsent(t *testing.T) {
	x := newFixture(t, nil, azblob.WriteOptions{})
	r, err := x.w.PutIfAbsent(context.Background(), "m.json", []byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if o, _ := x.f.Get("m.json"); string(o.Data) != `{"a":1}` || o.ETag != r.Identity || r.Size != 7 {
		t.Fatalf("stored %q ETag %s, receipt %+v", o.Data, o.ETag, r)
	}
	if _, err := x.w.PutIfAbsent(context.Background(), "m.json", []byte(`{"a":2}`)); !errors.Is(err, objfile.ErrExists) {
		t.Fatalf("second put: %v, want ErrExists", err)
	}
}

// TestDeleteIf: only the named version is deleted, by If-Match on its
// ETag.
func TestDeleteIf(t *testing.T) {
	x := newFixture(t, nil, azblob.WriteOptions{})
	r, err := upload(t, x.w, "obj", []byte("mine"))
	if err != nil {
		t.Fatal(err)
	}
	if err := x.w.DeleteIf(context.Background(), "obj", `"0x1"`); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("delete of another ETag: %v, want ErrObjectChanged", err)
	}
	if _, ok := x.f.Get("obj"); !ok {
		t.Fatal("deleted the wrong version")
	}
	if err := x.w.DeleteIf(context.Background(), "obj", r.Identity); err != nil {
		t.Fatal(err)
	}
	if _, ok := x.f.Get("obj"); ok {
		t.Fatal("not deleted")
	}
	if err := x.w.DeleteIf(context.Background(), "obj", r.Identity); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("delete of a gone blob: %v, want fs.ErrNotExist", err)
	}
	if err := x.w.DeleteIf(context.Background(), "obj", ""); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("delete by no identity: %v", err)
	}
}

// TestRootWriter: names are checked and written under the prefix; the
// receipt names the object as the caller did.
func TestRootWriter(t *testing.T) {
	x := newFixture(t, nil, azblob.WriteOptions{})
	rw := objfile.RootWriter(x.w, "ds/x")
	r, err := upload(t, rw, "data/p.parquet", objfiletest.Data(2*block+1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if r.Name != "data/p.parquet" {
		t.Fatalf("receipt names %q", r.Name)
	}
	if _, ok := x.f.Get("ds/x/data/p.parquet"); !ok {
		t.Fatalf("blobs: %v", x.f.Names())
	}
	if _, err := rw.NewWriter(context.Background(), "../escape", objfile.CreateOnly); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("a path CheckPath refuses: %v", err)
	}
}
