// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

const bucket = "bkt"

// fake serves the two calls the package makes: object metadata on the JSON
// API and a ranged read of one generation on the XML API. It keeps only each
// object's live generation, as a bucket without object versioning does.
type fake struct {
	mu      sync.Mutex
	gen     int64
	objects map[string]object
}

type object struct {
	gen  int64
	data []byte
}

func (f *fake) put(_ testing.TB, name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen++
	f.objects[name] = object{gen: f.gen, data: data}
}

func (f *fake) delete(_ testing.TB, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, name)
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if name, ok := strings.CutPrefix(r.URL.Path, "/storage/v1/b/"+bucket+"/o/"); ok {
		o, found := f.objects[name]
		if !found {
			http.Error(w, `{"error":{"code":404,"message":"No such object"}}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"bucket": bucket, "name": name, "size": strconv.Itoa(len(o.data)),
			"generation": strconv.FormatInt(o.gen, 10), "metageneration": "1",
		})
		return
	}
	name, ok := strings.CutPrefix(r.URL.Path, "/"+bucket+"/")
	o, found := f.objects[name]
	if gen := r.URL.Query().Get("generation"); !ok || !found || gen != "" && gen != strconv.FormatInt(o.gen, 10) {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	var start, end int
	if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || end >= len(o.data) {
		http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.Header().Set("X-Goog-Generation", strconv.FormatInt(o.gen, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(o.data[start : end+1])
}

func TestOpener(t *testing.T) {
	objfiletest.Run(t, func(t *testing.T, wrap func(http.Handler) http.Handler) objfiletest.Store {
		f := &fake{objects: map[string]object{}}
		srv := httptest.NewServer(wrap(f))
		t.Cleanup(srv.Close)
		client, err := storage.NewClient(context.Background(),
			option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return objfiletest.Store{Opener: gcs.New(client, bucket), Put: f.put, Delete: f.delete}
	})
}
