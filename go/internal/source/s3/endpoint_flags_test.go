// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"flag"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestEndpointURLDefaultsPathStyle: a custom endpoint addresses buckets
// path-style unless --path-style=false, as Java's --force-path-style
// default; the flag's outcome is what the identity records, and what the
// request is sent with.
func TestEndpointURLDefaultsPathStyle(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	var path, host atomic.Value
	srv := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.Path)
		host.Store(r.Host)
		w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	opened := func(args ...string) *opened[Object] {
		t.Helper()
		f := flag.NewFlagSet("t", flag.ContinueOnError)
		o := flags(f, false, keyrange.Flags(f, false))
		if err := f.Parse(append(args, "--anonymous")); err != nil {
			t.Fatal(err)
		}
		src, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: o, Workers: 1, Warn: func(string) {}})
		if err != nil {
			t.Fatal(err)
		}
		return src.(*opened[Object])
	}
	if o := opened("--endpoint", "http://store.example:9000"); !o.p.opt.PathStyle {
		t.Fatal("a custom endpoint is not path-style by default")
	}
	if o := opened("--endpoint", "http://store.example:9000", "--path-style=false"); o.p.opt.PathStyle {
		t.Fatal("--path-style=false is not virtual-host addressing")
	}
	if o := opened(); o.p.opt.PathStyle {
		t.Fatal("AWS's endpoint is path-style by default")
	}
	// The default reaches the request: the bucket in the path.
	o := opened("--endpoint", srv)
	if _, err := o.p.client.ListObjectsV2(context.Background(), &client.ListInput{Bucket: "bkt"}); err != nil {
		t.Fatal(err)
	}
	if path.Load() != "/bkt" || host.Load() != srv[len("http://"):] {
		t.Fatalf("request to %v%v", host.Load(), path.Load())
	}
}
