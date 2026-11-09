// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/source/keyrange/keyrangetest"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// differentialKeys are the provider oracle's keys (TestRowsMatchSDK) and
// the hostile encoding minefield (keyrangetest.Hostile).
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

// TestScannerConformsToSDKDecoder holds the production decoder, the
// scanner, to the SDK's own deserializer (the stock SDK client). Current
// objects: for every page of the fake bucket (the hostile keys, the
// oracle's request shapes, through each whole chain), the client's page
// equals the stock client's, every field; and every captured
// ListObjectsV2 body decodes to the same page. Versions: for every captured
// ListObjectVersions body, the scanner's entries equal the SDK's Versions
// and DeleteMarkers together, as multisets (the order, which the SDK loses,
// is TestVersionsKeepWireOrder's and TestStockSDKLosesVersionsOrder's).
func TestScannerConformsToSDKDecoder(t *testing.T) {
	s3test.NoCredentials(t)
	t.Run("fake bucket", func(t *testing.T) {
		url := s3test.Bucket(t, differentialKeys())
		c, sdk := newTest(t, testConfig(url)), stock(url)
		for _, start := range []ListInput{
			{MaxKeys: 97}, {MaxKeys: 1000, FetchOwner: true}, {Prefix: "data/year=2021/", MaxKeys: 100},
			{StartAfter: "a/x", MaxKeys: 50}, {Delimiter: "/", MaxKeys: 3}, {Prefix: "a/", Delimiter: "/"},
			{Prefix: "m/%", Delimiter: "/", MaxKeys: 7},
		} {
			in := start
			in.Bucket = s3test.Name
			n := 0
			for ; ; n++ {
				a, err := c.ListObjectsV2(context.Background(), &in)
				if err != nil {
					t.Fatal(err)
				}
				b, err := stockList(context.Background(), sdk, &in)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(a, b) {
					t.Fatalf("%+v page %d:\nscanner %+v\n    sdk %+v", in, n, a, b)
				}
				if !a.IsTruncated {
					break
				}
				in.StartAfter, in.ContinuationToken = "", a.NextContinuationToken
			}
			if n == 0 && start.MaxKeys < 10 && start.Prefix == "" {
				t.Fatalf("%+v: one page", start)
			}
		}
	})
	files, _ := filepath.Glob(filepath.Join("testdata", "versions", "*.xml"))
	if len(files) < 10 {
		t.Fatalf("fixtures: %v", files)
	}
	bodies := map[string][]byte{"bench list": benchBody(false), "bench versions": benchBody(true)}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		bodies[filepath.Base(f)] = b
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
			if strings.Contains(string(body), "<ListBucketResult") {
				want, err := parseList(body)
				if err != nil {
					t.Fatal(err)
				}
				got, err := stockList(context.Background(), stock(url), &ListInput{Bucket: "b"})
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("sdk %+v (%v)\nscanner %+v", got, err, want)
				}
				return
			}
			pg, err := parseVersions(body)
			if err != nil {
				t.Fatal(err)
			}
			out, err := stock(url).ListObjectVersions(context.Background(), &awss3.ListObjectVersionsInput{Bucket: aws.String("b")})
			if err != nil {
				t.Fatal(err)
			}
			got, err := stockVersions(out)
			if err != nil {
				t.Fatal(err)
			}
			if want := sortedVersions(pg.Versions); !reflect.DeepEqual(sortedVersions(got), want) {
				t.Fatalf("sdk %+v\nscanner %+v", sortedVersions(got), want)
			}
		})
	}
}

// stockVersions is the SDK's ListObjectVersions output as entries, the
// keys decoded once when the answer says encoding-type=url: its Versions,
// then its DeleteMarkers.
func stockVersions(out *awss3.ListObjectVersionsOutput) ([]Version, error) {
	enc := string(out.EncodingType)
	var vs []Version
	owner := func(o *Object, id, name *string) {
		o.OwnerID, o.OwnerDisplayName = aws.ToString(id), aws.ToString(name)
	}
	for _, v := range out.Versions {
		e := Version{Object: Object{Key: aws.ToString(v.Key), ETag: aws.ToString(v.ETag), Size: aws.ToInt64(v.Size),
			StorageClass: string(v.StorageClass), ChecksumType: string(v.ChecksumType)},
			VersionID: aws.ToString(v.VersionId), IsLatest: aws.ToBool(v.IsLatest)}
		if v.LastModified != nil {
			e.Mtime = v.LastModified.UnixMicro()
		}
		if v.Owner != nil {
			owner(&e.Object, v.Owner.ID, v.Owner.DisplayName)
		}
		if len(v.ChecksumAlgorithm) > 0 {
			e.ChecksumAlgorithm = string(v.ChecksumAlgorithm[0])
		}
		vs = append(vs, e)
	}
	for _, d := range out.DeleteMarkers {
		e := Version{Object: Object{Key: aws.ToString(d.Key)}, VersionID: aws.ToString(d.VersionId),
			IsLatest: aws.ToBool(d.IsLatest), DeleteMarker: true}
		if d.LastModified != nil {
			e.Mtime = d.LastModified.UnixMicro()
		}
		if d.Owner != nil {
			owner(&e.Object, d.Owner.ID, d.Owner.DisplayName)
		}
		vs = append(vs, e)
	}
	for i := range vs {
		var err error
		if vs[i].Key, err = stockUnescape(enc, vs[i].Key); err != nil {
			return nil, err
		}
	}
	return vs, nil
}

// sortedVersions is vs in a fixed order, for a multiset comparison.
func sortedVersions(vs []Version) []Version {
	vs = slices.Clone(vs)
	if vs == nil {
		vs = []Version{}
	}
	slices.SortFunc(vs, func(a, b Version) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), strings.Compare(a.VersionID, b.VersionID))
	})
	return vs
}

// TestCrossEnvelopeIsParseFailure: each operation requires its own
// envelope. A ListVersionsResult answering ListObjectsV2, or a
// ListBucketResult answering ListObjectVersions, is a parse failure,
// retried (the scanner checks it), never an empty final page.
func TestCrossEnvelopeIsParseFailure(t *testing.T) {
	s3test.NoCredentials(t)
	versions := versionsBody("VDV")
	list := `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>k</Key></Contents></ListBucketResult>`
	cfg := testConfig(s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("versions") {
			w.Write([]byte(list))
			return
		}
		w.Write([]byte(versions))
	})))
	cfg.MaxAttempts = 2
	c := newTest(t, cfg)
	_, lerr := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	_, verr := c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b"})
	if Classify(lerr) != objhttp.ClassParse || Classify(verr) != objhttp.ClassParse || c.Counters().ParseFailures != 4 {
		t.Fatalf("list %v, versions %v, %+v", lerr, verr, c.Counters())
	}
}
