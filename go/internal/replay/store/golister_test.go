// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// The Go lister's own datasets, written in-test by listing.Run against the
// in-process S3 and GCS fakes and against a local tree: the format-2
// datasets the replay server serves in production, with the lister's
// part, row-group, page and index-bound geometry.

// listerKeys are the bucket keys: byte-order edge cases (a key that is a
// prefix of the next, an empty segment, percent text, two spellings of é,
// a 4-byte UTF-8 rune), a bulk run long enough for several parts, and a
// dated tree for delimiter rollups.
func listerKeys() []string {
	keys := []string{"a", "a/", "a/b", "a/b/", "a/b/x", "a//c", "a/%2F", "é", "é", "\U00010000"}
	for i := range 1100 {
		keys = append(keys, fmt.Sprintf("bulk/k%04d", i))
	}
	for m := 1; m <= 2; m++ {
		for d := 1; d <= 3; d++ {
			keys = append(keys, fmt.Sprintf("logs/2026/%02d/day%02d.log", m, d))
		}
	}
	slices.SortFunc(keys, strings.Compare)
	return keys
}

// listInto runs the Go lister on uri with options into dir/name and
// returns the dataset directory. Small parts make several of them.
func listInto(t testing.TB, dir, name, uri string, options any) string {
	t.Helper()
	out := filepath.Join(dir, name)
	_, err := listing.Run(context.Background(), listing.Config{
		Source: listing.SourceConfig{URI: uri, Options: options},
		// A dataset is all these tests need: unjournaled where a journaled run is refused (Windows).
		Output: listing.OutputConfig{Dir: out, NoJournal: !dataset.DirSyncEnforced},
		Limits: listing.Limits{PartSize: 6 << 10, PartInterval: -1},
	})
	if err != nil {
		t.Fatalf("list %s: %v", uri, err)
	}
	return out
}

// listS3 lists listerKeys from the S3 fake.
func listS3(t testing.TB, dir string) string {
	t.Helper()
	s3test.NoCredentials(t)
	url := s3test.Bucket(t, listerKeys())
	return listInto(t, dir, "go-s3", "s3://"+s3test.Name, &s3.Options{Endpoint: url, PathStyle: true})
}

// listGCS lists listerKeys from the GCS fake, with full object resources
// (etag, storage class, the GCS-native columns).
func listGCS(t testing.TB, dir string) string {
	t.Helper()
	url := gcstest.NewStrict(listerKeys()).Serve(t)
	return listInto(t, dir, "go-gcs", "gs://bkt", &gcs.Options{Endpoint: url, Anonymous: true})
}

// listLocal lists a small local tree.
func listLocal(t testing.TB, dir string) string {
	t.Helper()
	tree := filepath.Join(dir, "tree")
	files := map[string]string{"a/b/x": "x", "a/b/y.txt": "yy", "a/z": "zzz", "docs/readme.md": "hello", "docs/sub/deep.txt": "deep!!", "top.txt": "t", "café.bin": "1234"}
	for name, body := range files {
		path := filepath.Join(tree, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return listInto(t, dir, "go-local", tree, &localfs.Options{})
}
