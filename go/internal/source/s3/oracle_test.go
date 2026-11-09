// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// sdkOracle is the AWS SDK's stock S3 client, with its own decoder, as the
// provider's API: the oracle the provider's rows are held to (the client's
// pages are held to the stock decoder by the client package's
// TestScannerConformsToSDKDecoder). It does not ask for
// encoding-type=url, so keys arrive raw.
type sdkOracle struct{ c *awss3.Client }

// newSDKOracle is an anonymous stock client for endpoint, path-style.
func newSDKOracle(endpoint string) *sdkOracle {
	return &sdkOracle{awss3.New(awss3.Options{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, UsePathStyle: true,
		BaseEndpoint: aws.String(endpoint)})}
}

func (s *sdkOracle) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	req := &awss3.ListObjectsV2Input{Bucket: aws.String(in.Bucket)}
	set := func(dst **string, v string) {
		if v != "" {
			*dst = aws.String(v)
		}
	}
	set(&req.Prefix, in.Prefix)
	set(&req.StartAfter, in.StartAfter)
	set(&req.ContinuationToken, in.ContinuationToken)
	set(&req.Delimiter, in.Delimiter)
	if in.MaxKeys > 0 {
		req.MaxKeys = aws.Int32(int32(in.MaxKeys))
	}
	if in.FetchOwner {
		req.FetchOwner = aws.Bool(true)
	}
	out, err := s.c.ListObjectsV2(ctx, req)
	if err != nil {
		return nil, err
	}
	pg := &client.Page{
		IsTruncated: aws.ToBool(out.IsTruncated), NextContinuationToken: aws.ToString(out.NextContinuationToken),
		Contents: make([]client.Object, len(out.Contents)),
	}
	for i, o := range out.Contents {
		c := &pg.Contents[i]
		c.Key, c.ETag, c.Size = aws.ToString(o.Key), aws.ToString(o.ETag), aws.ToInt64(o.Size)
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
	for _, p := range out.CommonPrefixes {
		pg.CommonPrefixes = append(pg.CommonPrefixes, aws.ToString(p.Prefix))
	}
	return pg, nil
}

// TestRowsMatchSDK lists through the client and the stock SDK client,
// sequential and parallel.
func TestRowsMatchSDK(t *testing.T) {
	keys := append(testKeys(), "sp ace", "pl+us", "pct%41", "amp&<>")
	url := fakeBucket(t, keys, 0)
	sdk := newSDKOracle(url)
	for _, seq := range []bool{true, false} {
		want, _ := list(t, Options{Bucket: "bkt", Client: sdk, FetchOwner: true, Workers: 4, Policy: keyrange.Policy{Sequential: seq}}, 4)
		got, _ := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Anonymous: true, FetchOwner: true, Workers: 4, Policy: keyrange.Policy{Sequential: seq}}, 4)
		if len(got) != len(keys) || len(got) != len(want) {
			t.Fatalf("rows %d, sdk %d, keys %d", len(got), len(want), len(keys))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("sequential=%v row %d: %+v vs %+v", seq, i, got[i], want[i])
			}
		}
	}
}
