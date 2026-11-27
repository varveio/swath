// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/internal/source/s3/s3test"
)

// recorder answers through the fake and records each request's method,
// path and signing region; with noCreate it refuses CreateBucket as an R2
// token scoped to one bucket does.
type recorder struct {
	h        http.Handler
	noCreate bool
	mu       sync.Mutex
	reqs     []string
	regions  map[string]bool
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, q *http.Request) {
	r.mu.Lock()
	r.reqs = append(r.reqs, q.Method+" "+q.URL.Path)
	if a := q.Header.Get("Authorization"); a != "" {
		// Credential=AKID/DATE/REGION/s3/aws4_request
		if f := strings.Split(strings.SplitN(strings.SplitN(a, "Credential=", 2)[1], ",", 2)[0], "/"); len(f) > 2 {
			r.regions[f[2]] = true
		}
	}
	r.mu.Unlock()
	if r.noCreate && q.Method == http.MethodPut && strings.Count(strings.Trim(q.URL.Path, "/"), "/") == 0 {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
		return
	}
	r.h.ServeHTTP(w, q)
}

func (r *recorder) count(prefix string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, q := range r.reqs {
		if strings.HasPrefix(q, prefix) {
			n++
		}
	}
	return n
}

func setup(t *testing.T, noCreate bool, buckets ...string) (*recorder, *s3mem.Backend, string, string) {
	t.Helper()
	s3test.NoCredentials(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_REGION", "")
	b := s3mem.New()
	for _, n := range buckets {
		if err := b.CreateBucket(n); err != nil {
			t.Fatal(err)
		}
	}
	rec := &recorder{h: s3test.Handler(b), noCreate: noCreate, regions: map[string]bool{}}
	keys := filepath.Join(t.TempDir(), "keys")
	if err := os.WriteFile(keys, []byte("a/1\na/2\nb/ 3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return rec, b, s3test.Serve(t, rec), keys
}

func objects(t *testing.T, b *s3mem.Backend, bucket string) []string {
	t.Helper()
	l, err := b.ListBucket(bucket, nil, gofakes3.ListBucketPage{})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, o := range l.Contents {
		out = append(out, o.Key)
	}
	return out
}

// On a store whose keys may not create buckets: no CreateBucket, requests
// signed for region auto, the marker a key under the prefix in the bucket
// itself, and a second run with the same file put nothing.
func TestFillOneBucket(t *testing.T) {
	rec, b, url, keys := setup(t, true, "bkt")
	sum, _ := fileSHA(keys)
	f := fill{endpoint: url, region: "auto", bucket: "bkt", keys: keys, workers: 4, markerPrefix: "_fill/"}
	var out bytes.Buffer
	if err := f.run(context.Background(), &out); err != nil {
		t.Fatalf("fill: %v\n%s", err, out.String())
	}
	if got, want := objects(t, b, "bkt"), []string{"_fill/" + sum, "a/1", "a/2", "b/ 3"}; !slices.Equal(got, want) {
		t.Fatalf("bucket holds %q, want %q", got, want)
	}
	if n := rec.count("PUT /bkt-fill"); n != 0 {
		t.Fatalf("%d requests to a marker bucket", n)
	}
	if n := rec.count("PUT /bkt/") + rec.count("PUT /bkt?"); n != 4 {
		t.Fatalf("%d object PUTs, want 3 keys and the marker", n)
	}
	if !rec.regions["auto"] || len(rec.regions) != 1 {
		t.Fatalf("requests signed for %v, want auto only", rec.regions)
	}
	if !strings.Contains(out.String(), "3 objects put, 0 refused") || !strings.Contains(out.String(), "requests sent: 5\n") {
		t.Fatalf("output:\n%s", out.String())
	}
	before := rec.count("PUT ")
	out.Reset()
	if err := f.run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if rec.count("PUT ") != before || !strings.Contains(out.String(), "already filled") {
		t.Fatalf("the second run put objects:\n%s", out.String())
	}
}

// The default (MinIO): both buckets created, the marker in BUCKET-fill,
// requests signed for us-east-1.
func TestFillDefault(t *testing.T) {
	rec, b, url, keys := setup(t, false)
	sum, _ := fileSHA(keys)
	f := fill{endpoint: url, region: "us-east-1", bucket: "bkt", keys: keys, workers: 4, create: true}
	var out bytes.Buffer
	if err := f.run(context.Background(), &out); err != nil {
		t.Fatalf("fill: %v\n%s", err, out.String())
	}
	if got, want := objects(t, b, "bkt"), []string{"a/1", "a/2", "b/ 3"}; !slices.Equal(got, want) {
		t.Fatalf("bucket holds %q, want %q", got, want)
	}
	if got := objects(t, b, "bkt-fill"); !slices.Equal(got, []string{sum}) {
		t.Fatalf("marker bucket holds %q, want the key file's sha256", got)
	}
	if !rec.regions["us-east-1"] || len(rec.regions) != 1 {
		t.Fatalf("requests signed for %v, want us-east-1 only", rec.regions)
	}
}

// Without -create=false a store that refuses CreateBucket stops the fill
// before any PUT of a key.
func TestFillCreateRefused(t *testing.T) {
	rec, _, url, keys := setup(t, true, "bkt")
	f := fill{endpoint: url, region: "auto", bucket: "bkt", keys: keys, workers: 4, create: true, markerPrefix: "_fill/"}
	err := f.run(context.Background(), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "create bkt") {
		t.Fatalf("err %v, want the refused CreateBucket", err)
	}
	if n := rec.count("PUT /bkt/"); n != 0 {
		t.Fatalf("%d keys put after the refusal", n)
	}
}
