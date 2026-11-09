// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"cmp"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestScannerConformsToSDKDecoder holds the production decoder, package
// client's scanner, to the SDK's own deserializer. Current objects: for
// every page of the fake bucket (the hostile keys, the oracle's request
// shapes) and every captured ListObjectsV2 body, the scanner's page equals
// the SDK's output converted (measure.go). Versions: for every captured
// ListObjectVersions body, the scanner's entries equal the SDK's Versions
// and DeleteMarkers together, as multisets (the order, which the SDK
// loses, is TestVersionsKeepWireOrder's and TestStockSDKLosesVersionsOrder's).
func TestScannerConformsToSDKDecoder(t *testing.T) {
	s3test.NoCredentials(t)
	t.Run("fake bucket", func(t *testing.T) {
		url := s3test.Bucket(t, differentialKeys())
		scanner, _ := newSDK(context.Background(), testConfig(url), false)
		sdk, _ := newSDK(context.Background(), testConfig(url), true)
		for _, start := range []client.ListInput{
			{MaxKeys: 97}, {MaxKeys: 1000, FetchOwner: true}, {Prefix: "data/year=2021/", MaxKeys: 100},
			{StartAfter: "a/x", MaxKeys: 50}, {Delimiter: "/", MaxKeys: 3}, {Prefix: "a/", Delimiter: "/"},
		} {
			in := start
			in.Bucket = s3test.Name
			for {
				a, err := scanner.ListObjectsV2(context.Background(), &in)
				if err != nil {
					t.Fatal(err)
				}
				b, err := sdk.ListObjectsV2(context.Background(), &in)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(a, b) {
					t.Fatalf("%+v:\nscanner %+v\n    sdk %+v", in, a, b)
				}
				if !a.IsTruncated {
					break
				}
				in.StartAfter, in.ContinuationToken = "", a.NextContinuationToken
			}
		}
	})
	files, _ := filepath.Glob(filepath.Join("..", "client", "testdata", "versions", "*.xml"))
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
				want, err := client.ParseList(body)
				if err != nil {
					t.Fatal(err)
				}
				sdk, _ := newSDK(context.Background(), testConfig(url), true)
				got, err := sdk.ListObjectsV2(context.Background(), &client.ListInput{Bucket: "b"})
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("sdk %+v (%v)\nscanner %+v", got, err, want)
				}
				return
			}
			pg, err := client.ParseVersions(body)
			if err != nil {
				t.Fatal(err)
			}
			stock := awss3.New(awss3.Options{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, UsePathStyle: true,
				BaseEndpoint: aws.String(url), Retryer: aws.NopRetryer{}})
			out, err := stock.ListObjectVersions(context.Background(), &awss3.ListObjectVersionsInput{Bucket: aws.String("b")})
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
func stockVersions(out *awss3.ListObjectVersionsOutput) ([]client.Version, error) {
	enc := string(out.EncodingType)
	var vs []client.Version
	owner := func(o *client.Object, id, name *string) {
		o.OwnerID, o.OwnerDisplayName = aws.ToString(id), aws.ToString(name)
	}
	for _, v := range out.Versions {
		e := client.Version{Object: client.Object{Key: aws.ToString(v.Key), ETag: aws.ToString(v.ETag), Size: aws.ToInt64(v.Size),
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
		e := client.Version{Object: client.Object{Key: aws.ToString(d.Key)}, VersionID: aws.ToString(d.VersionId),
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
		if vs[i].Key, err = unescape(enc, vs[i].Key); err != nil {
			return nil, err
		}
	}
	return vs, nil
}

// sortedVersions is vs in a fixed order, for a multiset comparison.
func sortedVersions(vs []client.Version) []client.Version {
	vs = slices.Clone(vs)
	if vs == nil {
		vs = []client.Version{}
	}
	slices.SortFunc(vs, func(a, b client.Version) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), strings.Compare(a.VersionID, b.VersionID))
	})
	return vs
}

// TestCrossEnvelopeIsParseFailure: each operation requires its own
// envelope. A ListVersionsResult answering ListObjectsV2, or a
// ListBucketResult answering ListObjectVersions, is a parse failure,
// retried, through both clients (the scanner checks it), never an empty
// final page.
func TestCrossEnvelopeIsParseFailure(t *testing.T) {
	s3test.NoCredentials(t)
	versions := versionsBody("VDV")
	list := `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>k</Key></Contents></ListBucketResult>`
	for _, d := range production {
		cfg := testConfig(s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Has("versions") {
				w.Write([]byte(list))
				return
			}
			w.Write([]byte(versions))
		})))
		cfg.MaxAttempts = 2
		hand, sdk := pair(t, cfg, d.dec, nil)
		for who, c := range map[string]lister{"hand-rolled": hand, "sdk": sdk} {
			_, lerr := c.ListObjectsV2(context.Background(), &client.ListInput{Bucket: "b"})
			_, verr := c.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"})
			if client.Classify(lerr) != objhttp.ClassParse || client.Classify(verr) != objhttp.ClassParse || c.Counters().ParseFailures != 4 {
				t.Fatalf("%s/%s: list %v, versions %v, %+v", d.name, who, lerr, verr, c.Counters())
			}
		}
	}
}
