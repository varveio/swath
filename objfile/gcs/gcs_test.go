// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/gcs"
	"github.com/varveio/swath/objfile/gcs/gcsfake"
	"github.com/varveio/swath/objfile/internal/objfiletest"
	"github.com/varveio/swath/objfile/internal/ranged"
)

const bucket = "bkt"

// client returns a client of a fake bucket served through wrap.
func client(t testing.TB, wrap func(http.Handler) http.Handler) (*gcsfake.Server, *storage.Client) {
	t.Helper()
	return clientOf(t, wrap, http.DefaultTransport)
}

// guardedClient is client with the validating transport remote.Open
// installs, as production reads have it.
func guardedClient(t testing.TB, wrap func(http.Handler) http.Handler) (*gcsfake.Server, *storage.Client) {
	t.Helper()
	return clientOf(t, wrap, ranged.Transport(http.DefaultTransport))
}

// observedClient is guardedClient with the observing transport below the
// validating one, as remote.Open builds it, recorded as observing.
func observedClient(t testing.TB, wrap func(http.Handler) http.Handler) (*gcsfake.Server, *storage.Client) {
	t.Helper()
	f, c := clientOf(t, wrap, ranged.Transport(ranged.Observe(http.DefaultTransport)))
	ranged.SetObserving(c, true)
	t.Cleanup(func() { ranged.SetObserving(c, false) })
	return f, c
}

func clientOf(t testing.TB, wrap func(http.Handler) http.Handler, tr http.RoundTripper) (*gcsfake.Server, *storage.Client) {
	t.Helper()
	f := gcsfake.New(bucket)
	srv := httptest.NewServer(wrap(f))
	t.Cleanup(srv.Close)
	c, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Transport: tr}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return f, c
}

// TestOpener runs the behaviour suite on a plain client and on one with
// the validating transport remote.Open installs.
func TestOpener(t *testing.T) {
	for _, c := range []struct {
		name      string
		newClient func(testing.TB, func(http.Handler) http.Handler) (*gcsfake.Server, *storage.Client)
	}{{"plain", client}, {"guarded", guardedClient}} {
		t.Run(c.name, func(t *testing.T) {
			objfiletest.Run(t, func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
				f, cl := c.newClient(t, wrap)
				return objfiletest.Store{Opener: gcs.New(cl, bucket),
					Put:    func(_ testing.TB, name string, data []byte) { f.Put(name, data) },
					Delete: func(_ testing.TB, name string) { f.Remove(name) }}
			})
		})
	}
}

// TestBudget: the requests a sized, batched and whole-object read make, and
// how their failures are classified, on a plain client and on the guarded
// one production reads use.
func TestBudget(t *testing.T) {
	for _, c := range []struct {
		name      string
		newClient func(testing.TB, func(http.Handler) http.Handler) (*gcsfake.Server, *storage.Client)
	}{{"plain", client}, {"guarded", guardedClient}} {
		t.Run(c.name, func(t *testing.T) {
			objfiletest.RunBudget(t, objfiletest.Budget{
				New: func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
					f, cl := c.newClient(t, wrap)
					return objfiletest.Store{Opener: gcs.New(cl, bucket),
						Put: func(_ testing.TB, name string, data []byte) { f.Put(name, data) }}
				},
				Observed: func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
					// Without the validating transport, the SDK's reader
					// reopens a cut body itself: observed as a retry too.
					f, cl := observedClient(t, wrap)
					if c.name == "plain" {
						f, cl = clientOf(t, wrap, ranged.Observe(http.DefaultTransport))
						ranged.SetObserving(cl, true)
						t.Cleanup(func() { ranged.SetObserving(cl, false) })
					}
					return objfiletest.Store{Opener: gcs.New(cl, bucket),
						Put: func(_ testing.TB, name string, data []byte) { f.Put(name, data) }}
				},
				Classify: func(r *http.Request) (objfiletest.Kind, bool) {
					pinned := r.URL.Query().Get("generation") != ""
					switch {
					case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/storage/v1/"):
						return objfiletest.Metadata, pinned
					case r.Method == http.MethodGet && r.Header.Get("Range") != "":
						return objfiletest.Ranged, pinned
					case r.Method == http.MethodGet:
						return objfiletest.Whole, pinned
					}
					return objfiletest.Other, pinned
				},
			})
		})
	}
}

// TestObservedChecksumFailure: a whole read whose CRC32C the SDK finds
// wrong at the body's end fails, and so does its observed attempt.
func TestObservedChecksumFailure(t *testing.T) {
	data := objfiletest.Data(3000, 1)
	tr := ranged.Transport(ranged.Observe(checksumTransport{data}))
	cl, err := storage.NewClient(context.Background(), option.WithEndpoint("http://store/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: tr}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	ranged.SetObserving(cl, true)
	t.Cleanup(func() { ranged.SetObserving(cl, false) })
	obs := &objfiletest.Observer{}
	ctx := objfile.WithObserver(context.Background(), obs)
	if _, _, err := gcs.New(cl, bucket).(objfile.ObjectReader).ReadObject(ctx, "manifest.json", 1<<20); err == nil {
		t.Fatal("a whole read with a wrong CRC32C succeeded")
	}
	reqs, ends := obs.Events()
	if len(reqs) == 0 || len(ends) != len(reqs) || ends[len(ends)-1].Err == nil {
		t.Fatalf("observed %+v, %+v; want the attempt failed", reqs, ends)
	}
}

// checksumTransport gives the real GCS SDK a complete payload with a bad CRC.
type checksumTransport struct{ data []byte }

func (tr checksumTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	h := make(http.Header)
	h.Set("X-Goog-Hash", "crc32c=AAAAAA==")
	h.Set("X-Goog-Generation", "1")
	h.Set("Content-Length", fmt.Sprint(len(tr.data)))
	return &http.Response{StatusCode: 200, Status: "200 OK", Header: h, ContentLength: int64(len(tr.data)), Body: io.NopCloser(strings.NewReader(string(tr.data))), Request: req}, nil
}

// TestObservedMalformedMetadata uses the SDK's JSON decoder, which discovers
// the invalid size after consuming the HTTP body's bytes.
func TestObservedMalformedMetadata(t *testing.T) {
	cl, err := storage.NewClient(context.Background(), option.WithEndpoint("http://store/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: ranged.Observe(badMetadataTransport{})}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	ranged.SetObserving(cl, true)
	t.Cleanup(func() { ranged.SetObserving(cl, false) })
	obs := &objfiletest.Observer{}
	_, err = gcs.New(cl, bucket).Open(objfile.WithObserver(context.Background(), obs), "obj")
	reqs, ends := obs.Events()
	if err == nil || len(reqs) != 1 || len(ends) != 1 || ends[0].Err == nil || ends[0].Bytes == 0 {
		t.Fatalf("Open = %v; observed %+v, %+v", err, reqs, ends)
	}
}

type badMetadataTransport struct{}

func (badMetadataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	const body = `{"size":"not an integer","generation":"1"}`
	return &http.Response{StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": []string{"application/json"}}, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}
