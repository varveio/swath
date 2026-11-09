// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// memory answers every request with body from memory: no network, so a
// List measures request building, the bounded read and decoding.
type memory struct{ body []byte }

func (m memory) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/xml"}}, Request: req,
		Body: io.NopCloser(bytes.NewReader(m.body)), ContentLength: int64(len(m.body))}, nil
}

// benchPage is a 5,000-entry page: blobs only, or with a delimiter half
// blobs and half prefixes, interleaved in order.
func benchPage(hierarchy bool) []byte {
	var b strings.Builder
	b.WriteString(listHead)
	for i := range MaxPageSize {
		if hierarchy && i%2 == 1 {
			fmt.Fprintf(&b, "<BlobPrefix><Name>data/%05d/</Name></BlobPrefix>", i)
			continue
		}
		fmt.Fprintf(&b, "<Blob><Name>data/%05d</Name><Properties><Creation-Time>Wed, 21 Oct 2026 17:27:04 GMT</Creation-Time>"+
			"<Last-Modified>Wed, 21 Oct 2026 17:27:05 GMT</Last-Modified><Etag>0x8DF2F9886625F76</Etag><Content-Length>%d</Content-Length>"+
			"<Content-Type>application/octet-stream</Content-Type><Content-MD5>2KAlPH8W1jwF0SkOJToFzg==</Content-MD5>"+
			"<BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>"+
			"<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState><ServerEncrypted>true</ServerEncrypted></Properties><OrMetadata /></Blob>", i, i)
	}
	b.WriteString("</Blobs><NextMarker>next</NextMarker></EnumerationResults>")
	return []byte(b.String())
}

// BenchmarkList is one full List call (request, bounded read, envelope
// check, the SDK's decoding, conversion, page validation) of a 5,000-entry
// page from memory: ns/object and allocs/page (allocs/op).
func BenchmarkList(b *testing.B) {
	for _, shape := range []string{"flat", "hierarchy"} {
		body := benchPage(shape == "hierarchy")
		in := ListInput{Container: "c", MaxResults: MaxPageSize}
		if shape == "hierarchy" {
			in.Delimiter = "/"
		}
		cfg := Config{Endpoint: "http://127.0.0.1:1", Account: "acct", Anonymous: true, MaxAttempts: 1,
			HTTPClient: &http.Client{Transport: memory{body}}}
		c, err := New(context.Background(), cfg)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(shape, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				p, err := c.List(context.Background(), &in)
				if err != nil || len(p.Items) != MaxPageSize {
					b.Fatalf("%v", err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*MaxPageSize), "ns/object")
		})
	}
}
