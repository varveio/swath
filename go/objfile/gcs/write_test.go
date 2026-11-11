// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

const chunk = 256 << 10 // the SDK's smallest chunk

func plain(h http.Handler) http.Handler { return h }

// writerOf is a Writer of a fake bucket with small chunks; retries counts
// the attempts its policy retried.
func writerOf(t *testing.T) (*gcsfake.Server, objfile.Writer, objfile.Opener, *atomic.Int64) {
	f, c := client(t, plain)
	var retries atomic.Int64
	w := gcs.NewWriter(c, bucket, gcs.WriteOptions{ChunkSize: chunk, Retried: func(string, error) { retries.Add(1) }})
	return f, w, gcs.New(c, bucket), &retries
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

// TestUpload: an object smaller than a chunk (one request) and one of
// several chunks (a resumable session) are created whole, with a receipt
// whose identity is the generation a read pins.
func TestUpload(t *testing.T) {
	for _, n := range []int{0, 1000, chunk, 4*chunk + 17} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			f, w, o, _ := writerOf(t)
			data := objfiletest.Data(n, 3)
			r, err := upload(t, w, "d/obj", data)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := f.Get("d/obj")
			if !ok || !bytes.Equal(got.Data, data) {
				t.Fatalf("stored %d bytes, want %d", len(got.Data), n)
			}
			if r.Name != "d/obj" || r.Size != int64(n) || r.Identity != strconv.FormatInt(got.Gen, 10) {
				t.Fatalf("receipt %+v, object generation %d", r, got.Gen)
			}
			if n > chunk && f.Count(gcsfake.OpSession) != 1 {
				t.Fatalf("%d sessions for a multi-chunk object", f.Count(gcsfake.OpSession))
			}
			if n == 0 {
				return
			}
			obj, err := o.Open(context.Background(), "d/obj")
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

// TestCreateOnly: an object already at the name, or created there while
// the upload runs, fails the upload with ErrExists and is left as it was.
func TestCreateOnly(t *testing.T) {
	for _, n := range []int{100, 3 * chunk} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			f, w, _, _ := writerOf(t)
			gen := f.Put("obj", []byte("theirs"))
			if _, err := upload(t, w, "obj", objfiletest.Data(n, 1)); !errors.Is(err, objfile.ErrExists) {
				t.Fatalf("upload over an object: %v, want ErrExists", err)
			}
			if o, _ := f.Get("obj"); o.Gen != gen || string(o.Data) != "theirs" {
				t.Fatal("the existing object changed")
			}
		})
	}
	t.Run("race", func(t *testing.T) {
		f, w, _, _ := writerOf(t)
		// Another writer creates the object after the session began: the
		// service checks the precondition again when it creates it.
		var once atomic.Bool
		f.SetFault(func(op, name string, after bool) int {
			if op == gcsfake.OpChunk && after && once.CompareAndSwap(false, true) {
				go f.Put(name, []byte("theirs")) // the fake's lock is held: put after this answer
			}
			return 0
		})
		if _, err := upload(t, w, "obj", objfiletest.Data(3*chunk, 1)); !errors.Is(err, objfile.ErrExists) {
			t.Fatalf("upload that lost a race: %v, want ErrExists", err)
		}
		if o, _ := f.Get("obj"); string(o.Data) != "theirs" {
			t.Fatal("the racing object was replaced")
		}
	})
}

// TestAbort: an aborted upload creates nothing, mid-chunk or before a byte
// was written, and its Close fails.
func TestAbort(t *testing.T) {
	for _, n := range []int{0, 100, 3 * chunk} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			f, w, _, _ := writerOf(t)
			u, err := w.NewWriter(context.Background(), "obj", objfile.CreateOnly)
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
			if _, ok := f.Get("obj"); ok {
				t.Fatal("an aborted upload created its object")
			}
			if _, ok := u.Receipt(); ok {
				t.Fatal("an aborted upload has a receipt")
			}
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		f, w, _, _ := writerOf(t)
		ctx, cancel := context.WithCancel(context.Background())
		u, err := w.NewWriter(ctx, "obj", objfile.CreateOnly)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = u.Write(objfiletest.Data(2*chunk, 2))
		cancel()
		if err := u.Close(); err == nil {
			t.Fatal("Close of a cancelled upload succeeded")
		}
		if _, ok := f.Get("obj"); ok {
			t.Fatal("a cancelled upload created its object")
		}
	})
}

// TestTransient: a chunk or a creation the service fails transiently is
// sent again (counted by Retried); a creation whose response was lost
// completes from its session, which answers with the object. A chunk that
// keeps failing past ChunkRetryDeadline ends the upload without an object.
func TestTransient(t *testing.T) {
	for _, op := range []string{gcsfake.OpSession, gcsfake.OpChunk, gcsfake.OpCreate} {
		for _, after := range []bool{false, true} {
			if op == gcsfake.OpSession && after {
				continue // a lost session answer starts another session; the first is never used
			}
			t.Run(op+"/after="+strconv.FormatBool(after), func(t *testing.T) {
				f, w, _, retries := writerOf(t)
				var n atomic.Int32
				f.SetFault(func(o, _ string, a bool) int {
					if o == op && a == after && n.Add(1) == 1 {
						return http.StatusServiceUnavailable
					}
					return 0
				})
				data := objfiletest.Data(3*chunk+5, 4)
				if _, err := upload(t, w, "obj", data); err != nil {
					t.Fatal(err)
				}
				if o, _ := f.Get("obj"); !bytes.Equal(o.Data, data) {
					t.Fatal("the object is not the bytes written")
				}
				if retries.Load() == 0 {
					t.Fatal("no retry counted")
				}
			})
		}
	}
	t.Run("bounded/single", func(t *testing.T) {
		f, c := client(t, plain)
		w := gcs.NewWriter(c, bucket, gcs.WriteOptions{ChunkSize: chunk, MaxAttempts: 3})
		f.SetFault(func(op, _ string, after bool) int {
			if op == gcsfake.OpCreate && !after {
				return http.StatusServiceUnavailable
			}
			return 0
		})
		if _, err := w.PutIfAbsent(context.Background(), "obj", []byte("x")); err == nil {
			t.Fatal("a creation that always fails succeeded")
		}
		if got := f.Count(gcsfake.OpCreate); got != 3 {
			t.Fatalf("%d attempts, want 3", got)
		}
	})
	t.Run("bounded", func(t *testing.T) {
		f, c := client(t, plain)
		w := gcs.NewWriter(c, bucket, gcs.WriteOptions{ChunkSize: chunk, MaxAttempts: 3, ChunkRetryDeadline: 300 * time.Millisecond})
		f.SetFault(func(op, _ string, after bool) int {
			if op == gcsfake.OpChunk && !after {
				return http.StatusServiceUnavailable
			}
			return 0
		})
		if _, err := upload(t, w, "obj", objfiletest.Data(3*chunk, 4)); err == nil {
			t.Fatal("an upload whose chunks always fail succeeded")
		}
		if got := f.Count(gcsfake.OpChunk); got < 2 {
			t.Fatalf("%d chunk attempts: the chunk was not retried", got)
		}
		if _, ok := f.Get("obj"); ok {
			t.Fatal("a failed upload created its object")
		}
	})
}

// TestPutIfAbsent: one request; an object already there is ErrExists, and
// so is the write's own object when the creation's response was lost and
// the retry found it: the caller reads it back to tell them apart.
func TestPutIfAbsent(t *testing.T) {
	f, w, _, _ := writerOf(t)
	r, err := w.PutIfAbsent(context.Background(), "m.json", []byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if o, _ := f.Get("m.json"); string(o.Data) != `{"a":1}` || strconv.FormatInt(o.Gen, 10) != r.Identity {
		t.Fatalf("stored %q gen %d, receipt %+v", o.Data, o.Gen, r)
	}
	if _, err := w.PutIfAbsent(context.Background(), "m.json", []byte(`{"a":2}`)); !errors.Is(err, objfile.ErrExists) {
		t.Fatalf("second put: %v, want ErrExists", err)
	}
	var lost atomic.Bool
	f.SetFault(func(op, _ string, after bool) int {
		if op == gcsfake.OpCreate && after && lost.CompareAndSwap(false, true) {
			return http.StatusServiceUnavailable
		}
		return 0
	})
	if _, err := w.PutIfAbsent(context.Background(), "n.json", []byte(`{"b":1}`)); !errors.Is(err, objfile.ErrExists) {
		t.Fatalf("put whose response was lost: %v, want ErrExists from the retry", err)
	}
	if o, ok := f.Get("n.json"); !ok || string(o.Data) != `{"b":1}` {
		t.Fatal("the put whose response was lost did not create its object")
	}
}

// TestDeleteIf: only the named generation is deleted.
func TestDeleteIf(t *testing.T) {
	f, w, _, _ := writerOf(t)
	r, err := upload(t, w, "obj", []byte("mine"))
	if err != nil {
		t.Fatal(err)
	}
	gen, _ := strconv.ParseInt(r.Identity, 10, 64)
	if err := w.DeleteIf(context.Background(), "obj", strconv.FormatInt(gen+100, 10)); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("delete of another generation: %v, want ErrObjectChanged", err)
	}
	if _, ok := f.Get("obj"); !ok {
		t.Fatal("deleted the wrong generation")
	}
	if err := w.DeleteIf(context.Background(), "obj", r.Identity); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Get("obj"); ok {
		t.Fatal("not deleted")
	}
	if err := w.DeleteIf(context.Background(), "obj", r.Identity); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("delete of a gone object: %v, want fs.ErrNotExist", err)
	}
	if err := w.DeleteIf(context.Background(), "obj", "x"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("delete by a malformed identity: %v", err)
	}
}

// TestRootWriter: names are checked and written under the prefix; the
// receipt names the object as the caller did.
func TestRootWriter(t *testing.T) {
	f, w, _, _ := writerOf(t)
	rw := objfile.RootWriter(w, "ds/x")
	r, err := upload(t, rw, "data/p.parquet", []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Name != "data/p.parquet" {
		t.Fatalf("receipt names %q", r.Name)
	}
	if _, ok := f.Get("ds/x/data/p.parquet"); !ok {
		t.Fatalf("objects: %v", f.Names())
	}
	if _, err := rw.NewWriter(context.Background(), "../escape", objfile.CreateOnly); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("a path CheckPath refuses: %v", err)
	}
	if _, err := rw.PutIfAbsent(context.Background(), "m.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Get("ds/x/m.json"); !ok {
		t.Fatalf("objects: %v", f.Names())
	}
}

// failing fails, as a connection lost in transport would, a resumable
// session's middle chunks (chunks), its last request (last), or every
// single-request creation (single).
type failing struct {
	base                 http.RoundTripper
	chunks, last, single bool
}

func (f failing) RoundTrip(r *http.Request) (*http.Response, error) {
	q := r.URL.Query()
	final := !strings.HasSuffix(r.Header.Get("Content-Range"), "/*")
	if q.Get("upload_id") != "" && (f.chunks && !final || f.last && final) || f.single && q.Get("uploadType") == "multipart" {
		return nil, errors.New("connection lost")
	}
	return f.base.RoundTrip(r)
}

// TestSessionSecrecy: a session URI is a bearer capability for its upload;
// no error a write returns quotes it, whether a middle chunk or the
// completion fails in transport. A completion that fails so is unresolved
// (the request may have reached the service); a failed chunk is not.
func TestSessionSecrecy(t *testing.T) {
	for _, c := range []struct {
		name       string
		size       int
		fail       failing
		unresolved bool
	}{
		{"chunk", 3 * chunk, failing{chunks: true}, false},
		{"completion", 3*chunk + 7, failing{last: true}, true},
		{"single", 100, failing{single: true}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := gcsfake.New(bucket)
			srv := httptest.NewServer(f)
			t.Cleanup(srv.Close)
			c.fail.base = http.DefaultTransport
			cl, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication(),
				option.WithHTTPClient(&http.Client{Transport: c.fail}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cl.Close() })
			w := gcs.NewWriter(cl, bucket, gcs.WriteOptions{ChunkSize: chunk, MaxAttempts: 1, ChunkRetryDeadline: 200 * time.Millisecond})
			_, err = upload(t, w, "obj", objfiletest.Data(c.size, 1))
			if err == nil {
				t.Fatal("a write whose requests fail succeeded")
			}
			if strings.Contains(err.Error(), "upload_id") || strings.Contains(err.Error(), "uploadType") {
				t.Fatalf("the request URL's query is in the error: %v", err)
			}
			if errors.Is(err, objfile.ErrUnresolved) != c.unresolved {
				t.Fatalf("unresolved = %v, want %v: %v", errors.Is(err, objfile.ErrUnresolved), c.unresolved, err)
			}
		})
	}
}

// TestUnresolved: a completion answered with a server error, or never
// answered, is unresolved; one the service refuses (403) is not.
func TestUnresolved(t *testing.T) {
	for code, want := range map[int]bool{http.StatusServiceUnavailable: true, http.StatusInternalServerError: true, http.StatusForbidden: false, http.StatusBadRequest: false} {
		f, w, _, _ := writerOf(t)
		f.SetFault(func(op, _ string, after bool) int {
			if op == gcsfake.OpCreate && !after {
				return code
			}
			return 0
		})
		_, err := w.PutIfAbsent(context.Background(), "m", []byte("x"))
		if err == nil || errors.Is(err, objfile.ErrUnresolved) != want {
			t.Errorf("%d: %v, unresolved want %v", code, err, want)
		}
	}
}

// TestUnresolvedSticks: a completion refused (403) after an attempt that
// failed ambiguously (503, retried by the SDK) is still unresolved: the
// first attempt may yet create the object.
func TestUnresolvedSticks(t *testing.T) {
	f, w, _, _ := writerOf(t)
	var n atomic.Int64
	f.SetFault(func(op, _ string, after bool) int {
		if op == gcsfake.OpCreate && !after {
			if n.Add(1) == 1 {
				return http.StatusServiceUnavailable
			}
			return http.StatusForbidden
		}
		return 0
	})
	if _, err := w.PutIfAbsent(context.Background(), "m", []byte("x")); !errors.Is(err, objfile.ErrUnresolved) {
		t.Fatalf("a refusal after an ambiguous attempt: %v", err)
	}
}

// TestConcurrentAbort: Abort from another goroutine ends a writer that
// keeps writing, without a data race and without waiting on the network.
func TestConcurrentAbort(t *testing.T) {
	_, c := client(t, plain)
	w := gcs.NewWriter(c, bucket, gcs.WriteOptions{ChunkSize: chunk})
	for i := range 10 {
		u, err := w.NewWriter(context.Background(), "race"+strconv.Itoa(i), objfile.CreateOnly)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := u.Write([]byte("first")); err != nil {
			t.Fatal(err)
		}
		started, done := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			close(started)
			for {
				if _, err := u.Write(make([]byte, 4096)); err != nil {
					return
				}
			}
		}()
		<-started
		if err := u.Abort(); err != nil {
			t.Fatal(err)
		}
		<-done
		if err := u.Close(); err == nil {
			t.Fatal("Close after Abort succeeded")
		}
	}
}
