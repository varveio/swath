// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

const containerName = "ctr"

// fake serves the two calls the package makes on a container: Get Blob
// Properties and a ranged Get Blob, which honours If-Match.
type fake struct {
	mu    sync.Mutex
	etag  int
	blobs map[string]blobData
}

type blobData struct {
	etag string
	data []byte
}

func (f *fake) put(_ testing.TB, name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.etag++
	f.blobs[name] = blobData{etag: fmt.Sprintf(`"0x%X"`, f.etag), data: data}
}

func (f *fake) delete(_ testing.TB, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.blobs, name)
}

func fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(status)
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, ok := strings.CutPrefix(r.URL.Path, "/"+containerName+"/")
	b, found := f.blobs[name]
	if !ok || !found {
		fail(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	w.Header().Set("ETag", b.etag)
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	switch r.Method {
	case http.MethodHead:
		w.Header().Set("Content-Length", strconv.Itoa(len(b.data)))
	case http.MethodGet:
		if m := r.Header.Get("If-Match"); m != "" && m != b.etag {
			fail(w, http.StatusPreconditionFailed, "ConditionNotMet")
			return
		}
		var start, end int
		if _, err := fmt.Sscanf(r.Header.Get("x-ms-range"), "bytes=%d-%d", &start, &end); err != nil || end >= len(b.data) {
			fail(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(b.data)))
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(b.data[start : end+1])
	default:
		fail(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb")
	}
}

func TestOpener(t *testing.T) {
	objfiletest.Run(t, func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
		f := &fake{blobs: map[string]blobData{}}
		srv := httptest.NewServer(wrap(f))
		t.Cleanup(srv.Close)
		client, err := azsdk.NewClientWithNoCredential(srv.URL+"/", &azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return objfiletest.Store{Opener: azblob.New(client, containerName), Put: f.put, Delete: f.delete}
	})
}
