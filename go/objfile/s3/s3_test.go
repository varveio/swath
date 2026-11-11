// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
	"github.com/varveio/swath/go/objfile/s3"
)

const bucket = "bkt"

// fake is a gofakes3 bucket behind an httptest server and an SDK client.
type fake struct {
	backend *s3mem.Backend
	client  *awss3.Client
}

func newFake(t *testing.T, wrap func(http.Handler) http.Handler) fake {
	t.Helper()
	b := s3mem.New()
	if err := b.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(wrap(gofakes3.New(b).Server()))
	t.Cleanup(srv.Close)
	client := awss3.New(awss3.Options{
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Region:       "us-east-1",
		Credentials:  aws.AnonymousCredentials{},
	})
	return fake{backend: b, client: client}
}

func (f fake) put(t testing.TB, name string, data []byte) {
	t.Helper()
	if _, err := f.backend.PutObject(bucket, name, nil, bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
}

// ifMatch makes the fake answer a GET whose If-Match names another ETag than
// the version it serves with 412, as S3 does; gofakes3 checks If-Match only on
// writes.
func ifMatch(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := r.Header.Get("If-Match")
		if r.Method != http.MethodGet || want == "" {
			h.ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code/100 == 2 && rec.Header().Get("ETag") != want {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		maps.Copy(w.Header(), rec.Header())
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	})
}

// unversioned drops the version id gofakes3 gives objects of a bucket without
// versioning on some responses; S3 sends none there.
func unversioned(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(dropVersion{w}, r)
		w.Header().Del("x-amz-version-id") // a response with no body or status written
	})
}

type dropVersion struct{ http.ResponseWriter }

func (d dropVersion) WriteHeader(code int) {
	d.Header().Del("x-amz-version-id")
	d.ResponseWriter.WriteHeader(code)
}

func (d dropVersion) Write(b []byte) (int, error) {
	d.Header().Del("x-amz-version-id")
	return d.ResponseWriter.Write(b)
}

func (d dropVersion) Flush() { d.ResponseWriter.(http.Flusher).Flush() }

func TestOpener(t *testing.T) {
	objfiletest.Run(t, func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
		f := newFake(t, func(h http.Handler) http.Handler { return wrap(unversioned(ifMatch(h))) })
		return objfiletest.Store{
			Opener: s3.New(f.client, bucket),
			Put:    f.put,
			Delete: func(t testing.TB, name string) {
				if _, err := f.backend.DeleteObject(bucket, name); err != nil {
					t.Fatal(err)
				}
			},
		}
	})
}

// TestSendsIfMatch: every read carries the ETag Open saw.
func TestSendsIfMatch(t *testing.T) {
	var sent []string
	f := newFake(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				sent = append(sent, r.Header.Get("If-Match"))
			}
			h.ServeHTTP(w, r)
		})
	})
	f.put(t, "obj", objfiletest.Data(100, 1))
	file, err := s3.New(f.client, bucket).Open(context.Background(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(make([]byte, 10), 5); err != nil {
		t.Fatal(err)
	}
	head, err := f.client.HeadObject(context.Background(), &awss3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj")})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0] != aws.ToString(head.ETag) {
		t.Fatalf("If-Match sent %q", sent)
	}
}

// TestETagCheck: a store that ignores If-Match still cannot hand back another
// version's bytes; the response's ETag is checked.
func TestETagCheck(t *testing.T) {
	f := newFake(t, func(h http.Handler) http.Handler { return h })
	f.put(t, "obj", objfiletest.Data(100, 1))
	file, err := s3.New(f.client, bucket).Open(context.Background(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	f.put(t, "obj", objfiletest.Data(100, 2))
	p := make([]byte, 10)
	if n, err := file.ReadAt(p, 0); !errors.Is(err, objfile.ErrObjectChanged) || n != 0 || !bytes.Equal(p, make([]byte, 10)) {
		t.Fatalf("ReadAt = %d, %v; want 0, ErrObjectChanged", n, err)
	}
}

// TestVersionPinned: in a versioned bucket the Object reads the version it
// opened, after the key has a newer one.
func TestVersionPinned(t *testing.T) {
	f := newFake(t, ifMatch)
	if err := f.backend.SetVersioningConfiguration(bucket, gofakes3.VersioningConfiguration{Status: gofakes3.VersioningEnabled}); err != nil {
		t.Fatal(err)
	}
	old := objfiletest.Data(100, 1)
	f.put(t, "obj", old)
	file, err := s3.New(f.client, bucket).Open(context.Background(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	f.put(t, "obj", objfiletest.Data(100, 2))
	p := make([]byte, 100)
	if _, err := file.ReadAt(p, 0); err != nil || !bytes.Equal(p, old) {
		t.Fatalf("ReadAt of the pinned version: %v, same bytes %v", err, bytes.Equal(p, old))
	}
}

// TestNullVersion: an object written before versioning was enabled has the
// version id "null", and the Object reads that version by it after the key is
// overwritten. gofakes3 never reports "null", so a handler plays S3.
func TestNullVersion(t *testing.T) {
	old, current := objfiletest.Data(10, 1), objfiletest.Data(10, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, etag, version := current, `"b"`, "v2"
		if r.URL.Query().Get("versionId") == "null" || r.Method == http.MethodHead {
			data, etag, version = old, `"a"`, "null"
		}
		if m := r.Header.Get("If-Match"); m != "" && m != etag {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("x-amz-version-id", version)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	client := awss3.New(awss3.Options{BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Region: "us-east-1", Credentials: aws.AnonymousCredentials{}})
	file, err := s3.New(client, bucket).Open(context.Background(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 10)
	if _, err := file.ReadAt(p, 0); err != nil || !bytes.Equal(p, old) || file.Identity() != `"a"` {
		t.Fatalf("ReadAt of the null version: %v, same bytes %v, Identity %q", err, bytes.Equal(p, old), file.Identity())
	}
}

// TestNullVersionIdentity: overwriting an object at version "null" (a
// versioning-suspended bucket) keeps the version id "null", so Identity is the
// ETag, a replacement of the same size has another Identity, and an Object
// pinned before the overwrite, from Open or OpenSized, reads none of the new
// bytes (If-Match fails). An object with a real version id is identified by it.
func TestNullVersionIdentity(t *testing.T) {
	var data []byte
	var etag, version string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m := r.Header.Get("If-Match"); m != "" && m != etag {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("x-amz-version-id", version)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	client := awss3.New(awss3.Options{BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Region: "us-east-1", Credentials: aws.AnonymousCredentials{}})
	open := func() objfile.Object {
		t.Helper()
		file, err := s3.New(client, bucket).Open(context.Background(), "obj")
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	data, etag, version = objfiletest.Data(10, 1), `"a"`, "null"
	opened := open()
	sized, err := s3.New(client, bucket).OpenSized(context.Background(), "obj", 10)
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 10)
	if _, err := sized.ReadAt(p, 0); err != nil || sized.Identity() != `"a"` {
		t.Fatalf("first read of the sized object: %v, Identity %q", err, sized.Identity())
	}
	data, etag = objfiletest.Data(10, 2), `"b"`
	for name, file := range map[string]objfile.Object{"Open": opened, "OpenSized": sized} {
		q := make([]byte, 10)
		if n, err := file.ReadAt(q, 0); n != 0 || !errors.Is(err, objfile.ErrObjectChanged) || !bytes.Equal(q, make([]byte, 10)) {
			t.Fatalf("%s: ReadAt after a same-size overwrite at version null = %d, %v", name, n, err)
		}
	}
	if first, second := opened.Identity(), open().Identity(); first != `"a"` || second != `"b"` {
		t.Fatalf("Identity of a null version before and after a same-size overwrite = %q, %q; want the ETags", first, second)
	}
	version = "v3"
	if id := open().Identity(); id != "v3" {
		t.Fatalf("Identity of version v3 = %q", id)
	}
}

// TestBudget: the requests a sized, batched and whole-object read make.
func TestBudget(t *testing.T) {
	objfiletest.RunBudget(t, objfiletest.Budget{
		New: func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
			f := newFake(t, func(h http.Handler) http.Handler { return wrap(unversioned(ifMatch(h))) })
			return objfiletest.Store{Opener: s3.New(f.client, bucket), Put: f.put}
		},
		Classify: func(r *http.Request) (objfiletest.Kind, bool) {
			pinned := r.Header.Get("If-Match") != ""
			switch {
			case r.Method == http.MethodHead:
				return objfiletest.Metadata, pinned
			case r.Method == http.MethodGet && r.Header.Get("Range") != "":
				return objfiletest.Ranged, pinned
			case r.Method == http.MethodGet:
				return objfiletest.Whole, pinned
			}
			return objfiletest.Other, pinned
		},
		UnknownTotalOK: true,
	})
}
