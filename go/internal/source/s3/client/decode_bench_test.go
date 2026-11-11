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
	"runtime"
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
// iteration through the client over an in-memory 1,000-entry answer (the
// bodies of BenchmarkDecodeBody):
// request building, signing (anonymous), endpoint resolution, the Guard's
// read and validation, the SDK's decoding of the minimal envelope and the
// scanner's of the captured body.
func BenchmarkDecode(b *testing.B) {
	for _, c := range []struct {
		name     string
		versions bool
		body     []byte
	}{
		{"scanner/list", false, benchBody(false)},
		{"scanner/versions", true, benchBody(true)},
		{"scanner/blockchain", false, shapedBody(false)},
		{"scanner/full", false, shapedBody(true)},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.Setenv("AWS_EC2_METADATA_DISABLED", "true")
			cfg := testConfig("http://127.0.0.1:1")
			cfg.HTTPClient = &http.Client{Transport: canned(c.body)}
			cl := newTest(b, cfg)
			ctx := context.Background()
			b.ReportAllocs()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.ResetTimer()
			for range b.N {
				n := 0
				if c.versions {
					pg, err := cl.ListObjectVersions(ctx, &VersionsInput{Bucket: "bkt"})
					if err != nil {
						b.Fatal(err)
					}
					n = len(pg.Versions)
				} else {
					pg, err := cl.ListObjectsV2(ctx, &ListInput{Bucket: "bkt"})
					if err != nil {
						b.Fatal(err)
					}
					n = len(pg.Contents)
				}
				if n != benchEntries {
					b.Fatalf("%d entries", n)
				}
			}
			b.StopTimer()
			runtime.ReadMemStats(&after)
			perEntry(b, &before, &after)
		})
	}
}

// perEntry reports the benchmark's time, bytes and allocations per entry.
func perEntry(b *testing.B, before, after *runtime.MemStats) {
	n := float64(b.N * benchEntries)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/n, "ns/object")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/n, "B/object")
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/n, "allocs/object")
}

// blockchainKey is the i-th key of a page shaped like aws-public-blockchain's
// (v1.0/btc/transactions/date=.../part-NNNNN-<uuid>-c000.snappy.parquet),
// 75 to 80 bytes.
func blockchainKey(i int) string {
	return fmt.Sprintf("v1.0/btc/transactions/date=2023-%02d-%02d/part-%05d-%08x-%04x-%04x-%04x-%012x-c000.snappy.parquet",
		1+i/28%12, 1+i%28, i%200, uint32(i)*2654435761, i%65536, 0x4000|i%4096, 0x8000|i%16384, uint64(i)*0x9E3779B97F4A7C15>>16)
}

// s3URLEscape is S3's encoding-type=url grammar: A-Z a-z 0-9 - _ . / * as
// they are, a space as '+', every other byte %XX upper-case (the replay
// server's output.AppendPercentEncoded).
func s3URLEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9', strings.IndexByte("-_./*", c) >= 0:
			b.WriteByte(c)
		case c == ' ':
			b.WriteByte('+')
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// shapedBody is a 1,000-entry ListBucketResult in the bytes the replay
// server writes (internal/replay/s3.RenderList: declaration and newline,
// element order, &quot;-quoted ETags, millisecond LastModified,
// encoding-type=url keys; Owner, when asked for, before StorageClass, as
// live S3 writes it):
//
//   - blockchain: keys like aws-public-blockchain's (every key holds an
//     escaped '='), single-part ETags, STANDARD, no checksum, no owner;
//   - full: the same keys with every optional field: ChecksumAlgorithm and
//     ChecksumType, Owner (fetch-owner), a multipart ETag on every fifth
//     entry, and every third key also holding a space and a two-byte UTF-8
//     character.
func shapedBody(full bool) []byte {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	b.WriteString(`<Name>aws-public-blockchain</Name><Prefix>v1.0%2Fbtc%2F</Prefix>`)
	b.WriteString(`<ContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</ContinuationToken>`)
	b.WriteString(`<NextContinuationToken>1hT6Y8bFh0rKQ2pZcTq/3D5vLm9wXn4sJk7aG1eU0Rcw=</NextContinuationToken>`)
	fmt.Fprintf(&b, `<KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>`, benchEntries)
	for i := range benchEntries {
		key := blockchainKey(i)
		if full && i%3 == 0 {
			key = strings.Replace(key, "transactions", "transactions é", 1)
		}
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>2023-%02d-%02dT%02d:%02d:%02d.000Z</LastModified>`,
			s3URLEscape(key), 1+i/28%12, 1+i%28, i%24, i%60, (i*7)%60)
		etag := fmt.Sprintf("%032x", uint64(i)*0x9E3779B97F4A7C15)
		if full && i%5 == 0 {
			etag = fmt.Sprintf("%032x-%d", uint64(i)*0x9E3779B97F4A7C15, 2+i%40)
		}
		fmt.Fprintf(&b, `<ETag>&quot;%s&quot;</ETag>`, etag)
		if full {
			b.WriteString(`<ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType>`)
		}
		fmt.Fprintf(&b, `<Size>%d</Size>`, 100000+i*7919)
		if full {
			b.WriteString(`<Owner><ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID><DisplayName>owner</DisplayName></Owner>`)
		}
		b.WriteString(`<StorageClass>STANDARD</StorageClass></Contents>`)
	}
	b.WriteString(`</ListBucketResult>`)
	return b.Bytes()
}

// BenchmarkDecodeBody is the scanner alone over the same bodies (no
// request), and over the replay-shaped pages of shapedBody, reporting
// cost per entry.
func BenchmarkDecodeBody(b *testing.B) {
	list, vers := benchBody(false), benchBody(true)
	chain, full := shapedBody(false), shapedBody(true)
	for _, c := range []struct {
		name string
		f    func() error
	}{
		{"scanner/list", func() error { _, err := parseList(list); return err }},
		{"scanner/versions", func() error { _, err := parseVersions(vers); return err }},
		{"scanner/blockchain", func() error { _, err := parseList(chain); return err }},
		{"scanner/full", func() error { _, err := parseList(full); return err }},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for range b.N {
				if err := c.f(); err != nil {
					b.Fatal(err)
				}
			}
			runtime.ReadMemStats(&after)
			perEntry(b, &before, &after)
		})
	}
}
