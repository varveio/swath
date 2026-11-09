// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

const benchEntries = 1000

// benchKey is the i-th key of a realistic page, 60 to 100 bytes, every
// fourth one holding characters encoding-type=url escapes.
func benchKey(i int) string {
	k := fmt.Sprintf("warehouse/events/year=2026/month=%02d/day=%02d/part-%05d-%s.snappy.parquet", 1+i%12, 1+i%28, i, strings.Repeat("c", i%30))
	if i%4 == 0 {
		k = strings.Replace(k, "events", "events and logs+%é", 1)
	}
	return k
}

// benchBody is a 1,000-entry page, URL-encoded keys, owners and checksums:
// a ListBucketResult, or a ListVersionsResult with a delete marker for
// every third entry.
func benchBody(versions bool) []byte {
	var b bytes.Buffer
	root := "ListBucketResult"
	if versions {
		root = "ListVersionsResult"
	}
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<%s xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name><Prefix></Prefix>`, root)
	if versions {
		b.WriteString(`<KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker>`)
	} else {
		fmt.Fprintf(&b, `<KeyCount>%d</KeyCount>`, benchEntries)
	}
	b.WriteString(`<MaxKeys>1000</MaxKeys><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>`)
	if versions {
		fmt.Fprintf(&b, `<NextKeyMarker>%s</NextKeyMarker><NextVersionIdMarker>Vz.9</NextVersionIdMarker>`, url.QueryEscape(benchKey(benchEntries-1)))
	} else {
		b.WriteString(`<NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>`)
	}
	owner := `<Owner><ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID><DisplayName>owner</DisplayName></Owner>`
	for i := range benchEntries {
		key, when := url.QueryEscape(benchKey(i)), fmt.Sprintf("2026-%02d-%02dT10:%02d:%02d.000Z", 1+i%12, 1+i%28, i%60, i%59)
		switch {
		case !versions:
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>&quot;%032x&quot;</ETag><ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType><Size>%d</Size>%s<StorageClass>STANDARD</StorageClass></Contents>`,
				key, when, i*7919, 1000+i, owner)
		case i%3 == 2:
			fmt.Fprintf(&b, `<DeleteMarker><Key>%s</Key><VersionId>d%031x</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified>%s</DeleteMarker>`, key, i, when, owner)
		default:
			fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>v%031x</VersionId><IsLatest>%v</IsLatest><LastModified>%s</LastModified><ETag>&quot;%032x&quot;</ETag><ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType><Size>%d</Size><StorageClass>STANDARD</StorageClass>%s</Version>`,
				key, i, i%3 == 0, when, i*7919, 1000+i, owner)
		}
	}
	fmt.Fprintf(&b, `</%s>`, root)
	return b.Bytes()
}

// canned answers every request with body, in memory: the benchmark
// measures the client (SDK request path, Guard, decoding), not a network.
type canned []byte

func (c canned) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/xml"}}, ContentLength: int64(len(c)),
		Body: io.NopCloser(bytes.NewReader(c)), Request: req}, nil
}

// BenchmarkDecode is one full ListObjectsV2 or ListObjectVersions call per
// iteration through the client over an in-memory 1,000-entry answer:
// request building, signing (anonymous), endpoint resolution, the Guard's
// read and validation, the SDK's decoding of the minimal envelope and the
// scanner's of the captured body.
func BenchmarkDecode(b *testing.B) {
	for _, versions := range []bool{false, true} {
		name := "scanner/list"
		if versions {
			name = "scanner/versions"
		}
		b.Run(name, func(b *testing.B) {
			b.Setenv("AWS_EC2_METADATA_DISABLED", "true")
			cfg := testConfig("http://127.0.0.1:1")
			cfg.HTTPClient = &http.Client{Transport: canned(benchBody(versions))}
			c := newTest(b, cfg)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				n := 0
				if versions {
					pg, err := c.ListObjectVersions(ctx, &VersionsInput{Bucket: "bkt"})
					if err != nil {
						b.Fatal(err)
					}
					n = len(pg.Versions)
				} else {
					pg, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "bkt"})
					if err != nil {
						b.Fatal(err)
					}
					n = len(pg.Contents)
				}
				if n != benchEntries {
					b.Fatalf("%d entries", n)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchEntries), "ns/object")
		})
	}
}

// BenchmarkDecodeBody is the scanner alone over the same bodies (no
// request).
func BenchmarkDecodeBody(b *testing.B) {
	list, vers := benchBody(false), benchBody(true)
	for _, c := range []struct {
		name string
		f    func() error
	}{
		{"scanner/list", func() error { _, err := parseList(list); return err }},
		{"scanner/versions", func() error { _, err := parseVersions(vers); return err }},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if err := c.f(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchEntries), "ns/object")
		})
	}
}

// TestBenchBodiesAgree: the benchmark's bodies come back through the
// client as the scanner decodes them, whole.
func TestBenchBodiesAgree(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, versions := range []bool{false, true} {
		cfg := testConfig("http://127.0.0.1:1")
		cfg.HTTPClient = &http.Client{Transport: canned(benchBody(versions))}
		c := newTest(t, cfg)
		var got, want any
		var err, werr error
		if versions {
			got, err = c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "bkt"})
			want, werr = parseVersions(benchBody(versions))
		} else {
			got, err = c.ListObjectsV2(context.Background(), &ListInput{Bucket: "bkt"})
			want, werr = parseList(benchBody(versions))
		}
		if err != nil || werr != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("versions %v: the client's page differs from the scanner's (%v, %v)", versions, err, werr)
		}
	}
}
