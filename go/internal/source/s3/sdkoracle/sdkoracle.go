// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sdkoracle adapts the AWS SDK for Go v2 to the provider's API so
// tests can hold the custom client to it. Only tests import it: the SDK is
// not linked into the swath binary.
package sdkoracle

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/source/s3/client"
)

// Client lists with the SDK.
type Client struct{ c *awss3.Client }

// New builds an anonymous SDK client for an endpoint (path-style when asked).
func New(endpoint, region string, pathStyle bool) *Client {
	return &Client{awss3.New(awss3.Options{
		Region: region, Credentials: aws.AnonymousCredentials{}, UsePathStyle: pathStyle,
		BaseEndpoint: aws.String(endpoint),
	})}
}

// ListObjectsV2 sends the request through the SDK and converts the answer.
// The SDK is not asked for encoding-type=url, so keys arrive raw.
func (s *Client) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
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
		HasContinuation: out.NextContinuationToken != nil, KeyCount: int(aws.ToInt32(out.KeyCount)),
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
