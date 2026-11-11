// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"bytes"
	"context"
	"errors"
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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
	"github.com/varveio/swath/go/objfile/s3"
	"github.com/varveio/swath/go/objfile/s3/s3fake"
)

// part is the tests' part size: the fake's minimum is lowered to it, so a
// multipart upload takes kilobytes, not megabytes.
const part = 64 << 10

// writeFake is s3fake behind an httptest server (through wrap) and an SDK
// client whose retries do not wait.
func writeFake(t testing.TB, versioned bool, wrap func(http.Handler) http.Handler) (*s3fake.Server, *awss3.Client) {
	t.Helper()
	f := s3fake.New(bucket)
	f.Versioned, f.MinPartSize = versioned, part
	var h http.Handler = f
	if wrap != nil {
		h = wrap(f)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return f, awss3.New(awss3.Options{BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Region: "us-east-1", Credentials: aws.AnonymousCredentials{},
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return time.Millisecond, nil })
			o.RateLimiter = ratelimit.None
		})})
}

type fixture struct {
	f       *s3fake.Server
	w       objfile.Writer
	o       objfile.Opener
	retries *atomic.Int64
	ops     *sync.Map // retries by operation
}

func newFixture(t *testing.T, versioned bool, wrap func(http.Handler) http.Handler, o s3.WriteOptions) fixture {
	t.Helper()
	f, c := writeFake(t, versioned, wrap)
	var retries atomic.Int64
	var ops sync.Map
	if o.PartSize == 0 {
		o.PartSize, o.MinPartSize = part, part
	}
	o.Retried = func(op string, _ error) {
		retries.Add(1)
		n, _ := ops.LoadOrStore(op, new(atomic.Int64))
		n.(*atomic.Int64).Add(1)
	}
	w, err := s3.NewWriter(c, bucket, o)
	if err != nil {
		t.Fatal(err)
	}
	return fixture{f: f, w: w, o: s3.New(c, bucket), retries: &retries, ops: &ops}
}

func (x fixture) retried(op string) int64 {
	n, ok := x.ops.Load(op)
	if !ok {
		return 0
	}
	return n.(*atomic.Int64).Load()
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

// TestUpload: an object of at most a part is one PutObject, a larger one a
// multipart upload of full parts and a last; each is created whole, with a
// receipt whose identity is what a read pins (the ETag; the version id in a
// versioned bucket), and no upload is left open.
func TestUpload(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		for _, n := range []int{0, 1000, part, part + 1, 3*part + 17} {
			t.Run(strconv.Itoa(n)+"/versioned="+strconv.FormatBool(versioned), func(t *testing.T) {
				x := newFixture(t, versioned, nil, s3.WriteOptions{})
				data := objfiletest.Data(n, 3)
				r, err := upload(t, x.w, "d/obj", data)
				if err != nil {
					t.Fatal(err)
				}
				got, ok := x.f.Get("d/obj")
				if !ok || !bytes.Equal(got.Data, data) {
					t.Fatalf("stored %d bytes, want %d", len(got.Data), n)
				}
				want := got.ETag
				if versioned {
					want = got.Version
				}
				if r.Name != "d/obj" || r.Size != int64(n) || r.Identity != want {
					t.Fatalf("receipt %+v, object %s %s", r, got.ETag, got.Version)
				}
				multi := n > part
				if multi != strings.Contains(got.ETag, "-") || multi != (x.f.Count(s3fake.OpSession) == 1) {
					t.Fatalf("%d bytes: ETag %s, %d multipart uploads", n, got.ETag, x.f.Count(s3fake.OpSession))
				}
				if multi && x.f.Count(s3fake.OpChunk) != int64((n+part-1)/part) {
					t.Fatalf("%d parts sent for %d bytes", x.f.Count(s3fake.OpChunk), n)
				}
				if len(x.f.Uploads()) != 0 {
					t.Fatalf("uploads left open: %v", x.f.Uploads())
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
}

// TestPartLimits: the part size is refused outside S3's limits (below
// 5 MiB by default), an object of more parts than the limit fails at the
// part past it and leaves no upload, and a store's minimum part size is
// the store's to enforce: a writer whose parts are smaller fails at the
// completion, which is aborted.
func TestPartLimits(t *testing.T) {
	_, c := writeFake(t, false, nil)
	for _, o := range []s3.WriteOptions{{PartSize: s3.MinPartSize - 1}, {PartSize: s3.MaxPartSize + 1}, {PartSize: part, MinPartSize: part + 1}} {
		if _, err := s3.NewWriter(c, bucket, o); err == nil {
			t.Errorf("%+v: accepted", o)
		}
	}
	if w, err := s3.NewWriter(c, bucket, s3.WriteOptions{}); err != nil || w == nil {
		t.Fatalf("defaults: %v", err)
	}

	defer s3.SetMaxParts(3)()
	x := newFixture(t, false, nil, s3.WriteOptions{})
	if _, err := upload(t, x.w, "three", objfiletest.Data(3*part, 1)); err != nil {
		t.Fatalf("3 parts: %v", err)
	}
	if _, err := upload(t, x.w, "four", objfiletest.Data(3*part+1, 1)); err == nil || !strings.Contains(err.Error(), "exceeds 3 parts") {
		t.Fatalf("4 parts: %v", err)
	}
	if _, ok := x.f.Get("four"); ok || len(x.f.Uploads()) != 0 {
		t.Fatalf("the refused object exists, or its upload is open: %v", x.f.Uploads())
	}

	f, c := writeFake(t, false, nil)
	f.MinPartSize = 2 * part
	w, _ := s3.NewWriter(c, bucket, s3.WriteOptions{PartSize: part, MinPartSize: part})
	if _, err := upload(t, w, "small", objfiletest.Data(3*part, 1)); err == nil || !strings.Contains(err.Error(), "EntityTooSmall") {
		t.Fatalf("parts below the store's minimum: %v", err)
	}
	if _, ok := f.Get("small"); ok || len(f.Uploads()) != 0 {
		t.Fatalf("the refused object exists, or its upload is open: %v", f.Uploads())
	}
}

// TestCreateOnly: an object already at the name, or created there while a
// multipart upload runs, fails the upload with ErrExists (412) and is left
// as it was; the refused multipart upload is aborted.
func TestCreateOnly(t *testing.T) {
	for _, n := range []int{100, 3 * part} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			x := newFixture(t, false, nil, s3.WriteOptions{})
			theirs := x.f.Put("obj", []byte("theirs"))
			if _, err := upload(t, x.w, "obj", objfiletest.Data(n, 1)); !errors.Is(err, objfile.ErrExists) || errors.Is(err, objfile.ErrConflict) {
				t.Fatalf("upload over an object: %v, want ErrExists", err)
			}
			if o, _ := x.f.Get("obj"); o.ETag != theirs.ETag || len(x.f.Uploads()) != 0 {
				t.Fatalf("the existing object changed, or the upload is open: %v", x.f.Uploads())
			}
		})
	}
	t.Run("race", func(t *testing.T) {
		x := newFixture(t, false, nil, s3.WriteOptions{})
		var once atomic.Bool
		x.f.SetFault(func(op, name string, after bool) int {
			if op == s3fake.OpChunk && after && once.CompareAndSwap(false, true) {
				go x.f.Put(name, []byte("theirs")) // the fake's lock is held: put after this answer
			}
			return 0
		})
		if _, err := upload(t, x.w, "obj", objfiletest.Data(3*part, 1)); !errors.Is(err, objfile.ErrExists) {
			t.Fatalf("upload that lost a race: %v, want ErrExists", err)
		}
		if o, _ := x.f.Get("obj"); string(o.Data) != "theirs" || len(x.f.Uploads()) != 0 {
			t.Fatalf("the racing object was replaced, or the upload is open: %v", x.f.Uploads())
		}
	})
}

// TestConflict: S3's 409 ConditionalRequestConflict is retried on a
// PutObject (sent again whole) and counted; on a completion it ends the
// upload with ErrConflict, not ErrExists, and the upload is aborted: the
// caller restarts the object from its first byte.
func TestConflict(t *testing.T) {
	x := newFixture(t, false, nil, s3.WriteOptions{})
	var n atomic.Int32
	x.f.SetFault(func(op, _ string, after bool) int {
		if op == s3fake.OpCreate && !after && n.Add(1) == 1 {
			return http.StatusConflict
		}
		return 0
	})
	if _, err := upload(t, x.w, "small", []byte("x")); err != nil {
		t.Fatalf("PutObject after a 409: %v", err)
	}
	if x.retried("PutObject") != 1 {
		t.Fatalf("PutObject retries %d, want 1", x.retried("PutObject"))
	}
	n.Store(0)
	if _, err := x.w.PutIfAbsent(context.Background(), "m.json", []byte("{}")); err != nil {
		t.Fatalf("PutIfAbsent after a 409: %v", err)
	}
	n.Store(0)
	_, err := upload(t, x.w, "big", objfiletest.Data(3*part, 1))
	if !errors.Is(err, objfile.ErrConflict) || errors.Is(err, objfile.ErrExists) {
		t.Fatalf("completion refused with 409: %v, want ErrConflict", err)
	}
	if _, ok := x.f.Get("big"); ok || len(x.f.Uploads()) != 0 || x.retried("CompleteMultipartUpload") != 0 {
		t.Fatalf("object left, upload open (%v) or the completion retried", x.f.Uploads())
	}
	// Always 409: PutObject gives up within its attempts.
	x.f.SetFault(func(op, _ string, after bool) int {
		if op == s3fake.OpCreate && !after {
			return http.StatusConflict
		}
		return 0
	})
	before := x.f.Count(s3fake.OpCreate)
	if _, err := x.w.PutIfAbsent(context.Background(), "n.json", []byte("{}")); !errors.Is(err, objfile.ErrConflict) {
		t.Fatalf("PutIfAbsent under 409s: %v", err)
	}
	if got := x.f.Count(s3fake.OpCreate) - before; got != s3.DefaultMaxAttempts {
		t.Fatalf("%d attempts, want %d", got, s3.DefaultMaxAttempts)
	}
}

// TestTransient: a request the service fails transiently (503) is sent
// again and counted; a completion answered 200 with an error is retried
// as S3 asks. A request that keeps failing ends the upload after
// MaxAttempts, without an object or an open upload.
func TestTransient(t *testing.T) {
	for _, op := range []string{s3fake.OpSession, s3fake.OpChunk, s3fake.OpCreate} {
		for _, status := range []int{http.StatusServiceUnavailable, http.StatusOK} {
			if status == http.StatusOK && op != s3fake.OpCreate {
				continue
			}
			t.Run(op+"/"+strconv.Itoa(status), func(t *testing.T) {
				x := newFixture(t, false, nil, s3.WriteOptions{})
				var n atomic.Int32
				x.f.SetFault(func(o, _ string, after bool) int {
					if o == op && !after && n.Add(1) == 1 {
						return status
					}
					return 0
				})
				data := objfiletest.Data(3*part+5, 4)
				if _, err := upload(t, x.w, "obj", data); err != nil {
					t.Fatal(err)
				}
				if o, _ := x.f.Get("obj"); !bytes.Equal(o.Data, data) {
					t.Fatal("the object is not the bytes written")
				}
				if x.retries.Load() != 1 {
					t.Fatalf("%d retries counted, want 1", x.retries.Load())
				}
			})
		}
	}
	t.Run("bounded", func(t *testing.T) {
		x := newFixture(t, false, nil, s3.WriteOptions{PartSize: part, MinPartSize: part, MaxAttempts: 3})
		x.f.SetFault(func(op, _ string, after bool) int {
			if op == s3fake.OpChunk && !after {
				return http.StatusServiceUnavailable
			}
			return 0
		})
		if _, err := upload(t, x.w, "obj", objfiletest.Data(3*part, 4)); err == nil {
			t.Fatal("an upload whose parts always fail succeeded")
		}
		if got := x.f.Count(s3fake.OpChunk); got != 3 {
			t.Fatalf("%d part attempts, want 3", got)
		}
		if _, ok := x.f.Get("obj"); ok || len(x.f.Uploads()) != 0 {
			t.Fatalf("a failed upload created its object, or left its upload: %v", x.f.Uploads())
		}
	})
}

// TestUploadIDRedacted: a transport failure names the request's URL, which
// holds the multipart upload's id; the upload's error does not, and still
// tells a connection failure.
func TestUploadIDRedacted(t *testing.T) {
	x := newFixture(t, false, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("partNumber") == "2" {
				c, _, _ := w.(http.Hijacker).Hijack()
				_ = c.Close()
				return
			}
			h.ServeHTTP(w, r)
		})
	}, s3.WriteOptions{PartSize: part, MinPartSize: part, MaxAttempts: 2})
	_, err := upload(t, x.w, "obj", objfiletest.Data(3*part, 1))
	var ne net.Error
	var nerr *net.OpError
	if err == nil || strings.Contains(err.Error(), "upload-0") || !strings.Contains(err.Error(), "[upload-id]") {
		t.Fatalf("error %v: the upload id is shown, or the failure is not", err)
	}
	if !errors.As(err, &ne) && !errors.As(err, &nerr) && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("the connection failure's class is lost: %v", err)
	}
	if len(x.f.Uploads()) != 0 {
		t.Fatalf("the failed upload is open: %v", x.f.Uploads())
	}
}

// TestAbort: an aborted upload creates nothing, before a byte, in its
// buffer or after parts were sent (its multipart upload is aborted), and
// its Close fails; Abort after a successful Close does nothing.
func TestAbort(t *testing.T) {
	for _, n := range []int{0, 100, 3 * part} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			x := newFixture(t, false, nil, s3.WriteOptions{})
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
			if _, ok := x.f.Get("obj"); ok || len(x.f.Uploads()) != 0 {
				t.Fatalf("an aborted upload created its object, or is open: %v", x.f.Uploads())
			}
			if _, ok := u.Receipt(); ok {
				t.Fatal("an aborted upload has a receipt")
			}
		})
	}
	t.Run("after close", func(t *testing.T) {
		x := newFixture(t, false, nil, s3.WriteOptions{})
		u, _ := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
		_, _ = u.Write(objfiletest.Data(3*part, 2))
		if err := u.Close(); err != nil {
			t.Fatal(err)
		}
		if err := u.Abort(); err != nil {
			t.Fatal(err)
		}
		if _, ok := x.f.Get("obj"); !ok {
			t.Fatal("Abort after Close removed the object")
		}
		if _, ok := u.Receipt(); !ok {
			t.Fatal("no receipt")
		}
	})
	// The run is cancelled while a part is in flight: the upload's context
	// is gone, yet its multipart upload is aborted under its own.
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var x fixture
		x = newFixture(t, false, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("partNumber") == "2" {
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
		}, s3.WriteOptions{})
		u, err := x.w.NewWriter(ctx, "obj", objfile.CreateOnly)
		if err != nil {
			t.Fatal(err)
		}
		_, err = u.Write(objfiletest.Data(3*part, 2))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("write cancelled mid-part: %v", err)
		}
		if err := u.Close(); err == nil {
			t.Fatal("Close of a cancelled upload succeeded")
		}
		if _, ok := x.f.Get("obj"); ok || len(x.f.Uploads()) != 0 || x.f.Count(s3fake.OpAbort) != 1 {
			t.Fatalf("object made, or upload open (%v), %d aborts", x.f.Uploads(), x.f.Count(s3fake.OpAbort))
		}
	})
	// Abort from another goroutine while Write sends parts (go test -race).
	t.Run("concurrent", func(t *testing.T) {
		for range 20 {
			x := newFixture(t, false, nil, s3.WriteOptions{})
			u, _ := x.w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range 8 {
					if _, err := u.Write(objfiletest.Data(part/2, 1)); err != nil {
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
			if _, ok := x.f.Get("obj"); ok || len(x.f.Uploads()) != 0 {
				t.Fatalf("object made, or upload open: %v", x.f.Uploads())
			}
		}
	})
}

// TestPutIfAbsent: one request; an object already there is ErrExists, and
// so is the write's own object when the creation's response was lost and
// the retry found it: the caller reads it back to tell them apart.
func TestPutIfAbsent(t *testing.T) {
	x := newFixture(t, false, nil, s3.WriteOptions{})
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
	var lost atomic.Bool
	x.f.SetFault(func(op, _ string, after bool) int {
		if op == s3fake.OpCreate && after && lost.CompareAndSwap(false, true) {
			return http.StatusServiceUnavailable
		}
		return 0
	})
	if _, err := x.w.PutIfAbsent(context.Background(), "n.json", []byte(`{"b":1}`)); !errors.Is(err, objfile.ErrExists) {
		t.Fatalf("put whose response was lost: %v, want ErrExists from the retry", err)
	}
	if o, ok := x.f.Get("n.json"); !ok || string(o.Data) != `{"b":1}` {
		t.Fatal("the put whose response was lost did not create its object")
	}
}

// TestDeleteIf: only the named version is deleted: by If-Match on its
// ETag, or in a versioned bucket by its version id.
func TestDeleteIf(t *testing.T) {
	t.Run("etag", func(t *testing.T) {
		x := newFixture(t, false, nil, s3.WriteOptions{})
		r, err := upload(t, x.w, "obj", []byte("mine"))
		if err != nil {
			t.Fatal(err)
		}
		if err := x.w.DeleteIf(context.Background(), "obj", `"0123"`); !errors.Is(err, objfile.ErrObjectChanged) {
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
			t.Fatalf("delete of a gone object: %v, want fs.ErrNotExist", err)
		}
		if err := x.w.DeleteIf(context.Background(), "obj", ""); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("delete by no identity: %v", err)
		}
	})
	t.Run("version", func(t *testing.T) {
		x := newFixture(t, true, nil, s3.WriteOptions{})
		r, err := upload(t, x.w, "obj", []byte("mine"))
		if err != nil {
			t.Fatal(err)
		}
		theirs := x.f.Put("obj", []byte("theirs"))
		if err := x.w.DeleteIf(context.Background(), "obj", r.Identity); err != nil {
			t.Fatal(err)
		}
		if o, ok := x.f.Get("obj"); !ok || o.Version != theirs.Version {
			t.Fatal("deleting our version removed theirs")
		}
	})
}

// TestRootWriter: names are checked and written under the prefix; the
// receipt names the object as the caller did.
func TestRootWriter(t *testing.T) {
	x := newFixture(t, false, nil, s3.WriteOptions{})
	rw := objfile.RootWriter(x.w, "ds/x")
	r, err := upload(t, rw, "data/p.parquet", objfiletest.Data(2*part+1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if r.Name != "data/p.parquet" {
		t.Fatalf("receipt names %q", r.Name)
	}
	if _, ok := x.f.Get("ds/x/data/p.parquet"); !ok {
		t.Fatalf("objects: %v", x.f.Names())
	}
	if _, err := rw.NewWriter(context.Background(), "../escape", objfile.CreateOnly); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("a path CheckPath refuses: %v", err)
	}
}
