// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestFixturesThroughClient: the captured S3 answers in testdata
// (ListObjectVersions, and the ListObjectsV2 one), replayed over HTTP,
// come back through the client as the scanner decodes the body, every
// field, and none is an empty page but the captures of one.
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
			if strings.Contains(string(body), "<ListBucketResult") {
				got, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "b"})
				want, werr := ParseList(body)
				if err != nil || werr != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("client %+v (%v)\nscanner %+v (%v)", got, err, want, werr)
				}
				return
			}
			got, err := c.ListObjectVersions(ctx, &VersionsInput{Bucket: "b"})
			want, werr := ParseVersions(body)
			if err != nil || werr != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("client %+v (%v)\nscanner %+v (%v)", got, err, want, werr)
			}
			if len(got.Versions)+len(got.CommonPrefixes) == 0 && !strings.Contains(f, "past-end") && !strings.Contains(f, "zero-max") && !strings.Contains(f, "special-echo") {
				t.Fatalf("empty page from %s", f)
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
		if Classify(err) != objhttp.ClassInvalidPage || c.Counters().Attempts != 1 {
			t.Fatalf("%v (%q), %+v", err, Classify(err), c.Counters())
		}
	}
}
