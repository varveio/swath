// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/azblob"
	"github.com/varveio/swath/objfile/azblob/azfake"
	"github.com/varveio/swath/objfile/internal/objfiletest"
	"github.com/varveio/swath/objfile/internal/ranged"
)

const (
	account       = "acct"
	containerName = "ctr"
)

// fakeClient serves an azfake container through wrap (nil: as is) to a
// client of its account, addressed as an emulator is, whose retries do
// not wait.
func fakeClient(t testing.TB, wrap func(http.Handler) http.Handler) (*azfake.Server, *azsdk.Client) {
	t.Helper()
	return fakeClientVia(t, wrap, nil)
}

// observedClient is fakeClient with the observing transport objfile/remote
// installs, recorded as observing.
func observedClient(t testing.TB, wrap func(http.Handler) http.Handler) (*azfake.Server, *azsdk.Client) {
	t.Helper()
	f, c := fakeClientVia(t, wrap, ranged.Observe(http.DefaultTransport))
	ranged.SetObserving(c, true)
	t.Cleanup(func() { ranged.SetObserving(c, false) })
	return f, c
}

func fakeClientVia(t testing.TB, wrap func(http.Handler) http.Handler, tr http.RoundTripper) (*azfake.Server, *azsdk.Client) {
	t.Helper()
	f := azfake.New(account, containerName)
	var h http.Handler = f
	if wrap != nil {
		h = wrap(f)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	opts := azcore.ClientOptions{
		Retry: policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond},
	}
	if tr != nil {
		opts.Transport = &http.Client{Transport: tr}
	}
	client, err := azsdk.NewClientWithNoCredential(srv.URL+"/"+account+"/", &azsdk.ClientOptions{ClientOptions: opts})
	if err != nil {
		t.Fatal(err)
	}
	return f, client
}

func TestOpener(t *testing.T) {
	objfiletest.Run(t, func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
		f, client := fakeClient(t, wrap)
		return objfiletest.Store{Opener: azblob.New(client, containerName),
			Put:    func(_ testing.TB, name string, data []byte) { f.Put(name, data) },
			Delete: func(_ testing.TB, name string) { f.Remove(name) }}
	})
}

// TestBudget: the requests a sized, batched and whole-object read make.
func TestBudget(t *testing.T) {
	objfiletest.RunBudget(t, objfiletest.Budget{
		New: func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
			f, client := fakeClient(t, wrap)
			return objfiletest.Store{Opener: azblob.New(client, containerName),
				Put: func(_ testing.TB, name string, data []byte) { f.Put(name, data) }}
		},
		Observed: func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
			f, client := observedClient(t, wrap)
			return objfiletest.Store{Opener: azblob.New(client, containerName),
				Put: func(_ testing.TB, name string, data []byte) { f.Put(name, data) }}
		},
		Classify: func(r *http.Request) (objfiletest.Kind, bool) {
			pinned := r.Header.Get("If-Match") != ""
			switch {
			case r.Method == http.MethodHead:
				return objfiletest.Metadata, pinned
			case r.Method == http.MethodGet && (r.Header.Get("x-ms-range") != "" || r.Header.Get("Range") != ""):
				return objfiletest.Ranged, pinned
			case r.Method == http.MethodGet:
				return objfiletest.Whole, pinned
			}
			return objfiletest.Other, pinned
		},
		UnknownTotalOK: true,
	})
}

// TestPreconditionFailed: a 412 is ErrObjectChanged only for a request that
// sent the pin's If-Match; a first sized read or a whole-object read sent
// none, and its 412 is the store's error.
func TestPreconditionFailed(t *testing.T) {
	ctx := context.Background()
	f, client := fakeClient(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			h.ServeHTTP(w, r)
		})
	})
	f.Put("obj", objfiletest.Data(100, 1))
	o := azblob.New(client, containerName)
	sized, err := o.OpenSized(ctx, "obj", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sized.ReadAt(make([]byte, 10), 0); err == nil || errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("412 of a first sized read = %v; want the store's error, not ErrObjectChanged", err)
	}
	if _, _, err := o.(objfile.ObjectReader).ReadObject(ctx, "obj", 1000); err == nil || errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("412 of a whole-object read = %v; want the store's error, not ErrObjectChanged", err)
	}
	opened, err := o.Open(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.ReadAt(make([]byte, 10), 0); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("412 of a pinned read = %v; want ErrObjectChanged", err)
	}
}

// TestReadObjectStoredGzip: a blob stored with Content-Encoding: gzip is read
// whole as stored, not decompressed by Go's transport.
func TestReadObjectStoredGzip(t *testing.T) {
	var z bytes.Buffer
	gw := gzip.NewWriter(&z)
	_, _ = gw.Write(bytes.Repeat([]byte("manifest "), 100))
	_ = gw.Close()
	stored := z.Bytes()
	f, client := fakeClient(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip") // the blob's stored property
			h.ServeHTTP(w, r)
		})
	})
	f.Put("obj", stored)
	b, _, err := azblob.New(client, containerName).(objfile.ObjectReader).ReadObject(context.Background(), "obj", 1<<20)
	if err != nil || !bytes.Equal(b, stored) {
		t.Fatalf("ReadObject of a gzip-labelled blob = %d bytes, %v; want the %d stored", len(b), err, len(stored))
	}
}
