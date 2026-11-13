// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestEndpointURLDefaultsPathStyle: a custom endpoint addresses buckets
// path-style unless --no-s3-path-style, as Java's --force-path-style
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
		f := pflag.NewFlagSet("t", pflag.ContinueOnError)
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
	if o := opened("--endpoint-url", "http://store.example:9000"); !o.p.opt.PathStyle {
		t.Fatal("a custom endpoint is not path-style by default")
	}
	if o := opened("--endpoint-url", "http://store.example:9000", "--no-s3-path-style"); o.p.opt.PathStyle {
		t.Fatal("--no-s3-path-style is not virtual-host addressing")
	}
	if o := opened(); o.p.opt.PathStyle {
		t.Fatal("AWS's endpoint is path-style by default")
	}
	// The default reaches the request: the bucket in the path.
	o := opened("--endpoint-url", srv)
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
		{[]string{"--s3-path-style"}, true, true, false},
		{[]string{"--no-s3-path-style"}, false, true, false},
		{[]string{"--s3-path-style=true", "--no-s3-path-style"}, false, true, false},
		{[]string{"--no-s3-path-style", "--s3-path-style"}, true, true, false},
		{[]string{"--s3-path-style=invalid"}, false, false, true},
		{[]string{"--s3-path-style", "--s3-path-style=invalid"}, true, true, true},
	} {
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.SetOutput(io.Discard)
		opt := factory(t).RegisterFlags(fs, false).(*flagged)
		err := fs.Parse(c.args)
		if (err != nil) != c.bad || opt.PathStyle != c.value || opt.pathStyleSet != c.set {
			t.Errorf("%q: value=%v set=%v err=%v", c.args, opt.PathStyle, opt.pathStyleSet, err)
		}
		// Both spellings are switches: the help shows neither =true nor
		// =false.
		for _, n := range []string{"s3-path-style", "no-s3-path-style"} {
			if fl := fs.Lookup(n); fl == nil || fl.NoOptDefVal != "true" || source.Described(fl).Metavar != "" {
				t.Fatalf("--%s is not a switch", n)
			}
		}
	}
}

func TestBearerFlagConflicts(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--s3-bearer-token-refresh-interval=1m"}, "--s3-bearer-token-refresh-interval requires --s3-bearer-token-command"},
		{[]string{"--s3-bearer-token-command=unused", "--s3-profile=work"}, "--s3-bearer-token-command excludes --s3-profile"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			f := pflag.NewFlagSet("t", pflag.ContinueOnError)
			opt := factory(t).RegisterFlags(f, false)
			if err := f.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			opened, err := open(t.Context(), registry.Request{Root: "s3://bkt", Options: opt, Workers: 1})
			if opened != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("opened %v, error %v", opened, err)
			}
		})
	}
}
