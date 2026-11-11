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
)

const bucket = "bkt"

// client returns a client of a fake bucket served through wrap.
func client(t testing.TB, wrap func(http.Handler) http.Handler) (*gcsfake.Server, *storage.Client) {
	t.Helper()
	f := gcsfake.New(bucket)
	srv := httptest.NewServer(wrap(f))
	t.Cleanup(srv.Close)
	c, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return f, c
}

func TestOpener(t *testing.T) {
	objfiletest.Run(t, func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
		f, c := client(t, wrap)
		return objfiletest.Store{Opener: gcs.New(c, bucket),
			Put:    func(_ testing.TB, name string, data []byte) { f.Put(name, data) },
			Delete: func(_ testing.TB, name string) { f.Remove(name) }}
	})
}

// TestBudget: the requests a sized, batched and whole-object read make.
func TestBudget(t *testing.T) {
	objfiletest.RunBudget(t, objfiletest.Budget{
		New: func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
			f, c := client(t, wrap)
			return objfiletest.Store{Opener: gcs.New(c, bucket),
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
}
