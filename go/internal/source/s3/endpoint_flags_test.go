// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
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
		o := factory(t).RegisterFlags(f, false)
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

func TestPathStyleFlag(t *testing.T) {
	for _, c := range []struct {
		args            []string
		value, set, bad bool
	}{
		{nil, false, false, false},
		{[]string{"--path-style"}, true, true, false},
		{[]string{"--path-style=false"}, false, true, false},
		{[]string{"--path-style=true", "--path-style=false"}, false, true, false},
		{[]string{"--path-style=false", "--path-style"}, true, true, false},
		{[]string{"--path-style=invalid"}, false, false, true},
		{[]string{"--path-style", "--path-style=invalid"}, true, true, true},
	} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		opt := factory(t).RegisterFlags(fs, false).(*flagged)
		err := fs.Parse(c.args)
		if (err != nil) != c.bad || opt.PathStyle != c.value || opt.pathStyleSet != c.set {
			t.Errorf("%q: value=%v set=%v err=%v", c.args, opt.PathStyle, opt.pathStyleSet, err)
		}
		var help bytes.Buffer
		fs.SetOutput(&help)
		fs.PrintDefaults()
		if !strings.Contains(help.String(), "  -path-style\n    \tS3: path-style addressing (default: on with --endpoint, as Java's --force-path-style)\n") {
			t.Fatalf("path-style help changed: %s", help.String())
		}
	}
}

func TestBearerFlagRefusals(t *testing.T) {
	s3test.NoCredentials(t)
	for _, args := range [][]string{
		{"--bearer-token-refresh-interval=1m"},
		{"--bearer-token-command=never-run", "--profile=test"},
	} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		opt := factory(t).RegisterFlags(fs, false)
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		_, err := open(context.Background(), registry.Request{Root: "s3://bkt", Options: opt, Workers: 1})
		if !errors.Is(err, source.ErrUnsupported) {
			t.Fatalf("%q: %v", args, err)
		}
	}
}
