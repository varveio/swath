// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

// Measurement plumbing, removed after the comparison: SWATH_S3_DECODER=sdk
// has the SDK's deserializer decode ListObjectsV2 pages (versions stay the
// scanner's: the SDK cannot keep their order), and page converts its
// output, so one tagged binary can compare it with the scanner. It is not
// a production path: nothing checks the envelope there, and the SDK reads
// an empty body or another envelope as an empty final page.

import (
	"fmt"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// sdkDecode reports SWATH_S3_DECODER=sdk.
func sdkDecode() bool { return os.Getenv("SWATH_S3_DECODER") == "sdk" }

// page converts the SDK's ListObjectsV2 output, URL-decoding keys and
// common prefixes once when the answer says encoding-type=url (the SDK
// keeps them as sent).
func page(out *awss3.ListObjectsV2Output) (*client.Page, error) {
	enc := string(out.EncodingType)
	pg := &client.Page{
		IsTruncated: aws.ToBool(out.IsTruncated), NextContinuationToken: aws.ToString(out.NextContinuationToken),
		HasContinuation: out.NextContinuationToken != nil, KeyCount: int(aws.ToInt32(out.KeyCount)), EncodingType: enc,
		Contents: make([]client.Object, len(out.Contents)),
	}
	var err error
	for i, o := range out.Contents {
		c := &pg.Contents[i]
		if c.Key, err = unescape(enc, aws.ToString(o.Key)); err != nil {
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
			if pg.CommonPrefixes[i], err = unescape(enc, aws.ToString(p.Prefix)); err != nil {
				return nil, err
			}
		}
	}
	return pg, nil
}

// unescape decodes a key field once when enc is "url", with form semantics
// ('+' is a space) as S3's encoding-type=url answers need and as package
// client decodes them; a bad escape is ErrSyntax.
func unescape(enc, s string) (string, error) {
	if enc != "url" {
		return s, nil
	}
	v, err := url.QueryUnescape(s)
	if err != nil {
		return "", fmt.Errorf("%w: invalid URL escape in %q", objhttp.ErrSyntax, s)
	}
	return v, nil
}
