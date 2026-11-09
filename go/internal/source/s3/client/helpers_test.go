// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// testConfig is an anonymous path-style config for endpoint with tiny
// backoffs.
func testConfig(endpoint string) Config {
	return Config{Endpoint: endpoint, PathStyle: true, Anonymous: true, Region: "us-east-1",
		BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
}

// newTest is New(testConfig(endpoint)), failing the test on an error.
func newTest(t testing.TB, cfg Config) *Client {
	t.Helper()
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// requests is a counter view: what the answer tests compare.
func requests(c Counters) Counters {
	return Counters{Attempts: c.Attempts, Retries: c.Retries, Throttles: c.Throttles, Timeouts: c.Timeouts, ParseFailures: c.ParseFailures}
}

// stock is the AWS SDK's own S3 client with its own decoder, anonymous,
// path-style, without retries: the oracle the scanner is held to.
func stock(endpoint string) *awss3.Client {
	return awss3.New(awss3.Options{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, UsePathStyle: true,
		BaseEndpoint: aws.String(endpoint), Retryer: aws.NopRetryer{}})
}

// stockList is one ListObjectsV2 page through the stock SDK client
// (encoding-type=url), as a Page.
func stockList(ctx context.Context, c *awss3.Client, in *ListInput) (*Page, error) {
	req := &awss3.ListObjectsV2Input{Bucket: aws.String(in.Bucket), EncodingType: types.EncodingTypeUrl,
		Prefix: str(in.Prefix), StartAfter: str(in.StartAfter), ContinuationToken: str(in.ContinuationToken), Delimiter: str(in.Delimiter)}
	if in.MaxKeys > 0 {
		req.MaxKeys = aws.Int32(int32(in.MaxKeys))
	}
	if in.FetchOwner {
		req.FetchOwner = aws.Bool(true)
	}
	out, err := c.ListObjectsV2(ctx, req)
	if err != nil {
		return nil, err
	}
	return stockPage(out)
}

// stockPage converts the SDK's ListObjectsV2 output, URL-decoding keys and
// common prefixes once when the answer says encoding-type=url (the SDK
// keeps them as sent).
func stockPage(out *awss3.ListObjectsV2Output) (*Page, error) {
	enc := string(out.EncodingType)
	pg := &Page{
		IsTruncated: aws.ToBool(out.IsTruncated), NextContinuationToken: aws.ToString(out.NextContinuationToken),
		Contents: make([]Object, len(out.Contents)),
	}
	var err error
	for i, o := range out.Contents {
		c := &pg.Contents[i]
		if c.Key, err = stockUnescape(enc, aws.ToString(o.Key)); err != nil {
			return nil, err
		}
		c.ETag, c.Size = aws.ToString(o.ETag), aws.ToInt64(o.Size)
		if o.LastModified != nil {
			c.Mtime = o.LastModified.UnixMicro()
		}
		c.StorageClass = string(o.StorageClass)
		if o.Owner != nil {
			c.OwnerID, c.OwnerDisplayName = aws.ToString(o.Owner.ID), aws.ToString(o.Owner.DisplayName)
		}
		if len(o.ChecksumAlgorithm) > 0 {
			c.ChecksumAlgorithm = string(o.ChecksumAlgorithm[0])
		}
		c.ChecksumType = string(o.ChecksumType)
	}
	if len(out.CommonPrefixes) > 0 {
		pg.CommonPrefixes = make([]string, len(out.CommonPrefixes))
		for i, p := range out.CommonPrefixes {
			if pg.CommonPrefixes[i], err = stockUnescape(enc, aws.ToString(p.Prefix)); err != nil {
				return nil, err
			}
		}
	}
	return pg, nil
}

// stockUnescape decodes a key field once when enc is "url", with form
// semantics ('+' is a space), as the scanner decodes it.
func stockUnescape(enc, s string) (string, error) {
	if enc != "url" {
		return s, nil
	}
	v, err := url.QueryUnescape(s)
	if err != nil {
		return "", fmt.Errorf("invalid URL escape in %q: %w", s, err)
	}
	return v, nil
}
