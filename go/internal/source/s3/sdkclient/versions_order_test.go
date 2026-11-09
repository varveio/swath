// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

const sameTime = "2025-03-13T21:53:32.000Z"

// versionsBody is a truncated ListVersionsResult for one URL-encoded key
// whose entries, in the order kinds gives ('V' a <Version>, 'D' a
// <DeleteMarker>), share one LastModified; the first entry's version ID is
// "null".
func versionsBody(kinds string) string {
	b := `<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Name>b</Name><Prefix>a%2Bb+c</Prefix><KeyMarker>a%25</KeyMarker><VersionIdMarker></VersionIdMarker><MaxKeys>3</MaxKeys>` +
		`<EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>` +
		`<NextKeyMarker>a%2Bb+c%2F%25</NextKeyMarker><NextVersionIdMarker>v+2%2F</NextVersionIdMarker>`
	for i, k := range kinds {
		vid := fmt.Sprintf("v%d", i)
		if i == 0 {
			vid = "null"
		}
		if k == 'V' {
			b += `<Version><Key>a%2Bb+c%2F%25</Key><VersionId>` + vid + `</VersionId><IsLatest>` + fmt.Sprint(i == 0) +
				`</IsLatest><LastModified>` + sameTime + `</LastModified><ETag>&quot;e` + vid + `&quot;</ETag><Size>3</Size><StorageClass>STANDARD</StorageClass></Version>`
		} else {
			b += `<DeleteMarker><Key>a%2Bb+c%2F%25</Key><VersionId>` + vid + `</VersionId><IsLatest>` + fmt.Sprint(i == 0) +
				`</IsLatest><LastModified>` + sameTime + `</LastModified></DeleteMarker>`
		}
	}
	return b + `</ListVersionsResult>`
}

func serveVersions(t *testing.T, body string) string {
	return s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
}

// TestVersionsKeepWireOrder: a <Version>, <DeleteMarker>, <Version> run of
// one key with equal timestamps comes back in wire order through every
// decoder, as through the hand-rolled client: the "null" version ID
// verbatim, the key, NextKeyMarker and the echoed Prefix and KeyMarker
// decoded once ('+' a space), the NextVersionIdMarker verbatim.
func TestVersionsKeepWireOrder(t *testing.T) {
	s3test.NoCredentials(t)
	url := serveVersions(t, versionsBody("VDV"))
	for _, d := range decoders {
		hand, sdk := pair(t, testConfig(url), d.dec, nil)
		want, err := hand.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := sdk.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s:\n        sdk %+v\nhand-rolled %+v", d.name, got, want)
		}
		var kinds string
		for _, v := range got.Versions {
			if v.Key != "a+b c/%" || v.Mtime != got.Versions[0].Mtime {
				t.Fatalf("%s: %+v", d.name, v)
			}
			kinds += map[bool]string{false: "V", true: "D"}[v.DeleteMarker]
		}
		if kinds != "VDV" || got.Versions[0].VersionID != "null" || got.Versions[2].ETag != `"ev2"` ||
			got.NextKeyMarker != "a+b c/%" || got.NextVersionIDMarker != "v+2%2F" || got.Prefix != "a+b c" || got.KeyMarker != "a%" {
			t.Fatalf("%s: %s %+v", d.name, kinds, got)
		}
	}
}

// TestStockSDKLosesVersionsOrder is why versions keep their own decoder:
// the stock SDK output (Versions and DeleteMarkers, two lists) is the same
// for the wire orders V/D/V and V/V/D of one key with equal timestamps, so
// no merge of it can give back the order S3 sent.
func TestStockSDKLosesVersionsOrder(t *testing.T) {
	s3test.NoCredentials(t)
	stock := func(kinds string) *awss3.ListObjectVersionsOutput {
		t.Helper()
		body := versionsBody(kinds)
		c := awss3.New(awss3.Options{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, UsePathStyle: true,
			BaseEndpoint: aws.String(serveVersions(t, body)), Retryer: aws.NopRetryer{}})
		out, err := c.ListObjectVersions(context.Background(), &awss3.ListObjectVersionsInput{Bucket: aws.String("b")})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	vdv, vvd := stock("VDV"), stock("VVD")
	if len(vdv.Versions) != 2 || len(vdv.DeleteMarkers) != 1 {
		t.Fatalf("stock VDV: %d versions, %d delete markers", len(vdv.Versions), len(vdv.DeleteMarkers))
	}
	// Strip the IDs (they only name the entries) and compare what is left
	// for a merge to order by: keys, kinds, timestamps, latest flags.
	shape := func(o *awss3.ListObjectVersionsOutput) []string {
		var s []string
		for _, v := range o.Versions {
			s = append(s, fmt.Sprintf("V %s %s %v", aws.ToString(v.Key), v.LastModified, aws.ToBool(v.IsLatest)))
		}
		for _, d := range o.DeleteMarkers {
			s = append(s, fmt.Sprintf("D %s %s %v", aws.ToString(d.Key), d.LastModified, aws.ToBool(d.IsLatest)))
		}
		return s
	}
	if !reflect.DeepEqual(shape(vdv), shape(vvd)) {
		t.Fatalf("the stock outputs differ:\n%v\n%v", shape(vdv), shape(vvd))
	}
	for _, d := range decoders {
		_, sdkVDV := pair(t, testConfig(serveVersions(t, versionsBody("VDV"))), d.dec, nil)
		_, sdkVVD := pair(t, testConfig(serveVersions(t, versionsBody("VVD"))), d.dec, nil)
		a, _ := sdkVDV.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"})
		b, _ := sdkVVD.ListObjectVersions(context.Background(), &client.VersionsInput{Bucket: "b"})
		if a == nil || b == nil || !a.Versions[1].DeleteMarker || !b.Versions[2].DeleteMarker {
			t.Fatalf("%s: the adapter lost the order: %+v %+v", d.name, a, b)
		}
	}
}
