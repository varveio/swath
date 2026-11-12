// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
	"github.com/varveio/swath/go/objfile/internal/ranged"
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
