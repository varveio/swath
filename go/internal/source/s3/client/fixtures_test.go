// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestFixturesThroughClient: a wiring check. Each captured S3 answer in
// testdata (ListObjectVersions, and the ListObjectsV2 one), replayed over
// HTTP, comes back through the client with as many entries, prefixes and
// the truncation flag as its body holds: the client hands the captured
// body to the scanner. The scanner's field decoding is held to the SDK's by
// TestScannerConformsToSDKDecoder.
func TestFixturesThroughClient(t *testing.T) {
	s3test.NoCredentials(t)
	files, err := filepath.Glob(filepath.Join("testdata", "versions", "*.xml"))
	if err != nil || len(files) < 10 {
		t.Fatalf("fixtures: %v %v", files, err)
	}
	for _, f := range files {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(filepath.Base(f), func(t *testing.T) {
			srv := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
			c, ctx := newTest(t, testConfig(srv)), context.Background()
			n := func(element string) int { return strings.Count(string(body), "<"+element+">") }
			truncated := strings.Contains(string(body), "<IsTruncated>true</IsTruncated>")
			var entries, prefixes int
			var gotTruncated bool
			if strings.Contains(string(body), "<ListBucketResult") {
				pg, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "b"})
				if err != nil {
					t.Fatal(err)
				}
				entries, prefixes, gotTruncated = len(pg.Contents), len(pg.CommonPrefixes), pg.IsTruncated
			} else {
				pg, err := c.ListObjectVersions(ctx, &VersionsInput{Bucket: "b"})
				if err != nil {
					t.Fatal(err)
				}
				entries, prefixes, gotTruncated = len(pg.Versions), len(pg.CommonPrefixes), pg.IsTruncated
			}
			if want := n("Contents") + n("Version") + n("DeleteMarker"); entries != want || prefixes != n("CommonPrefixes") || gotTruncated != truncated {
				t.Fatalf("entries %d, prefixes %d, truncated %v; the body holds %d, %d, %v", entries, prefixes, gotTruncated, want, n("CommonPrefixes"), truncated)
			}
		})
	}
}

// TestValidateFailuresAreInvalidPage: a truncated versions page without a
// NextKeyMarker, or ending at it without a version-id marker, is
// invalid_page after one attempt, not retried.
func TestValidateFailuresAreInvalidPage(t *testing.T) {
	s3test.NoCredentials(t)
	for _, body := range []string{
		`<ListVersionsResult><IsTruncated>true</IsTruncated><Version><Key>k</Key><VersionId>v</VersionId></Version></ListVersionsResult>`,
		`<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>k</NextKeyMarker><Version><Key>k</Key><VersionId>v</VersionId></Version></ListVersionsResult>`,
	} {
		c := newTest(t, testConfig(s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))))
		_, err := c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b"})
		if !errors.Is(err, ErrInvalidPage) || c.Counters().Attempts != 1 {
			t.Fatalf("%v (%q), %+v", err, Classify(err), c.Counters())
		}
	}
}
