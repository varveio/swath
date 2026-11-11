// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/azblob/azfake"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
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
	f := azfake.New(account, containerName)
	var h http.Handler = f
	if wrap != nil {
		h = wrap(f)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	client, err := azsdk.NewClientWithNoCredential(srv.URL+"/"+account+"/", &azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{
		Retry: policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond},
	}})
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
