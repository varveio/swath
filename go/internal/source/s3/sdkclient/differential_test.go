// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/keyrange/keyrangetest"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// differentialKeys are the provider oracle's keys (TestClientPagesMatchSDK)
// and the hostile encoding minefield (keyrangetest.Hostile).
func differentialKeys() []string {
	keys := []string{"a", "a-b", "a.b", "a/", "a/x", "a/y/z", "a0", "ab", "b/c", "é", "日本/x", "z~", "\x7f",
		"sp ace", "pl+us", "pct%41", "amp&<>", "q\"'"}
	for i := range 500 {
		keys = append(keys, fmt.Sprintf("data/year=%d/part-%05d.parquet", 2020+i%5, i))
	}
	keys = append(keys, keyrangetest.Hostile()["encodingMinefield"]...)
	slices.Sort(keys)
	return slices.Compact(keys)
}

// TestPagesMatchHandRolled: on the fake bucket, the hand-rolled client and
// the SDK-backed one (every decoder) return deep-equal pages, every field,
// for the provider oracle's request shapes, through each whole chain.
func TestPagesMatchHandRolled(t *testing.T) {
	s3test.NoCredentials(t)
	url := s3test.Bucket(t, differentialKeys())
	for _, d := range decoders {
		t.Run(d.name, func(t *testing.T) {
			hand, sdk := pair(t, testConfig(url), d.dec, nil)
			for _, start := range []client.ListInput{
				{MaxKeys: 97},
				{MaxKeys: 1000, FetchOwner: true},
				{Prefix: "data/year=2021/", MaxKeys: 100},
				{StartAfter: "a/x", MaxKeys: 50},
				{Delimiter: "/", MaxKeys: 3},
				{Prefix: "a/", Delimiter: "/"},
				{Prefix: "m/%", Delimiter: "/", MaxKeys: 7},
			} {
				in := start
				in.Bucket = s3test.Name
				n := 0
				for ; ; n++ {
					a, err := hand.ListObjectsV2(context.Background(), &in)
					if err != nil {
						t.Fatal(err)
					}
					b, err := sdk.ListObjectsV2(context.Background(), &in)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(a, b) {
						t.Fatalf("%+v page %d:\nhand-rolled %+v\n        sdk %+v", in, n, a, b)
					}
					if !a.IsTruncated {
						break
					}
					in.StartAfter, in.ContinuationToken = "", a.NextContinuationToken
				}
				t.Logf("%+v: %d pages", start, n+1)
				if n == 0 && start.MaxKeys < 10 && start.Prefix == "" {
					t.Fatalf("%+v: one page", start)
				}
			}
		})
	}
}

// TestFixturesMatchHandRolled: the captured S3 answers in client/testdata
// (ListObjectVersions, and the ListObjectsV2 one), replayed over HTTP,
// decode to deep-equal pages (or the same failure class) through both
// clients and every decoder.
func TestFixturesMatchHandRolled(t *testing.T) {
	s3test.NoCredentials(t)
	dir := filepath.Join("..", "client", "testdata", "versions")
	files, err := filepath.Glob(filepath.Join(dir, "*.xml"))
	if err != nil || len(files) < 10 {
		t.Fatalf("fixtures: %v %v", files, err)
	}
	for _, f := range files {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		srv := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
		for _, d := range decoders {
			t.Run(filepath.Base(f)+"/"+d.name, func(t *testing.T) {
				hand, sdk := pair(t, testConfig(srv), d.dec, nil)
				ctx := context.Background()
				if strings.Contains(string(body), "<ListBucketResult") {
					a, aerr := hand.ListObjectsV2(ctx, &client.ListInput{Bucket: "b"})
					b, berr := sdk.ListObjectsV2(ctx, &client.ListInput{Bucket: "b"})
					if aerr != nil || berr != nil || !reflect.DeepEqual(a, b) {
						t.Fatalf("hand-rolled %+v (%v)\n        sdk %+v (%v)", a, aerr, b, berr)
					}
					return
				}
				a, aerr := hand.ListObjectVersions(ctx, &client.VersionsInput{Bucket: "b"})
				b, berr := sdk.ListObjectVersions(ctx, &client.VersionsInput{Bucket: "b"})
				if client.Classify(aerr) != client.Classify(berr) || !reflect.DeepEqual(a, b) {
					t.Fatalf("hand-rolled %+v (%v)\n        sdk %+v (%v)", a, aerr, b, berr)
				}
				if aerr == nil && len(a.Versions)+len(a.CommonPrefixes) == 0 && !strings.Contains(f, "past-end") && !strings.Contains(f, "zero-max") && !strings.Contains(f, "special-echo") {
					t.Fatalf("empty page from %s", f)
				}
			})
		}
	}
}

// TestValidateFailuresAreInvalidPage: a truncated versions page without a
// NextKeyMarker, or ending at it without a version-id marker, is
// invalid_page through every decoder, as through the hand-rolled client.
func TestValidateFailuresAreInvalidPage(t *testing.T) {
	s3test.NoCredentials(t)
	for _, body := range []string{
		`<ListVersionsResult><IsTruncated>true</IsTruncated><Version><Key>k</Key><VersionId>v</VersionId></Version></ListVersionsResult>`,
		`<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>k</NextKeyMarker><Version><Key>k</Key><VersionId>v</VersionId></Version></ListVersionsResult>`,
	} {
		srv := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
		for _, d := range decoders {
			hand, sdk := pair(t, testConfig(srv), d.dec, nil)
			_, aerr := hand.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"})
			_, berr := sdk.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"})
			if client.Classify(aerr) != objhttp.ClassInvalidPage || client.Classify(berr) != objhttp.ClassInvalidPage {
				t.Fatalf("%s: hand-rolled %v, sdk %v", d.name, aerr, berr)
			}
		}
	}
}
