// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"flag"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// TestEndpointURLDefaultsPathStyle: --endpoint-url is --endpoint, and a
// custom endpoint addresses buckets path-style unless --path-style=false,
// as Java's --force-path-style default.
func TestEndpointURLDefaultsPathStyle(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	route := func(args ...string) string {
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
		return src.(*opened).p.route
	}
	if r := route("--endpoint", "http://store.example:9000"); r != "http://store.example:9000/bkt" {
		t.Fatalf("default: %s", r)
	}
	if r := route("--endpoint", "http://store.example:9000", "--path-style=false"); r != "http://bkt.store.example:9000/" {
		t.Fatalf("virtual host: %s", r)
	}
	if r := route(); r != "https://bkt.s3.us-east-1.amazonaws.com/" {
		t.Fatalf("AWS: %s", r)
	}
}
