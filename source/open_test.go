// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/source"
	_ "github.com/varveio/swath/source/s3"
)

// TestOpenRegistered: an s3:// address opens through the s3 package this
// test links, with source.Options reaching it.
func TestOpenRegistered(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint := s3test.Bucket(t, []string{"a.txt"})
	ctx := context.Background()

	src, err := source.Open(ctx, "s3://"+s3test.Name+"/", source.Options{Endpoint: endpoint, PathStyle: true, Anonymous: true, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	page, err := src.Page(ctx, listing.PageRequest{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || string(page.Entries[0].Key) != "a.txt" {
		t.Errorf("entries %v, want a.txt", page.Entries)
	}
}

// TestOpenUnlinked: a known scheme whose package the program does not
// import is ErrUnsupported, naming what is linked and the import to add.
func TestOpenUnlinked(t *testing.T) {
	_, err := source.Open(context.Background(), "gs://bkt", source.Options{})
	if !errors.Is(err, listing.ErrUnsupported) {
		t.Fatalf("err %v, want ErrUnsupported", err)
	}
	for _, want := range []string{"gs://", "compiled in: s3", `import _ "github.com/varveio/swath/source/gcs"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err %q does not say %q", err, want)
		}
	}
}

// TestOpenInvalid: an address of an unknown scheme, with a key prefix, an
// empty name, a query or fragment, or of the wrong shape is
// ErrInvalidRequest, whether or not its provider is linked.
func TestOpenInvalid(t *testing.T) {
	for _, address := range []string{
		"ftp://bkt",
		"bkt",
		"s3://bkt/logs/",
		"gs://bkt/a",
		"az://account",
		"az://account/container/logs",
		"gs://",
		"s3:///",
		"az:///container",
		"az://account/",
		"gs://bkt?userProject=p",
		"s3://bkt#frag",
	} {
		if _, err := source.Open(context.Background(), address, source.Options{}); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("%s: err %v, want ErrInvalidRequest", address, err)
		}
	}
}
