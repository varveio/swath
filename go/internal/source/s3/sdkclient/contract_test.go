// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/objhttp/objhttptest"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

type contractClient struct{ *Client }

func (c contractClient) List(ctx context.Context) error {
	_, err := c.ListObjectsV2(ctx, &client.ListInput{Bucket: "b"})
	return err
}

const okPage = `<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>k%20</Key><Size>1</Size></Contents></ListBucketResult>`

// TestRetryContract: the SDK-backed client (every decoder) passes the shared retry and
// classification contract with S3's XML answers.
func TestRetryContract(t *testing.T) {
	xmlError := func(status int, code string) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.WriteHeader(status)
			fmt.Fprintf(w, `<Error><Code>%s</Code><Message>m</Message></Error>`, code)
		}
	}
	wire := objhttptest.Wire{
		OK:       func(w http.ResponseWriter) { fmt.Fprint(w, okPage) },
		Throttle: xmlError(503, "SlowDown"),
		Server:   xmlError(500, "InternalError"),
		Denied:   xmlError(403, "AccessDenied"),
		Garbled:  func(w http.ResponseWriter) { fmt.Fprint(w, `<ListBucketResult><IsTruncated>fal`) },
	}
	for _, d := range decoders {
		t.Run(d.name, func(t *testing.T) {
			objhttptest.Contract(t, wire, func(s objhttptest.Settings) objhttptest.Client {
				c, err := New(context.Background(), client.Config{Endpoint: s.Endpoint, PathStyle: true, Anonymous: true, Region: "us-east-1", RequestTimeout: s.Timeout,
					MaxAttempts: s.Retry.MaxAttempts, BaseBackoff: s.Retry.BaseBackoff, MaxBackoff: s.Retry.MaxBackoff, Limiter: s.Retry.Limiter}, WithDecoder(d.dec))
				if err != nil {
					t.Fatal(err)
				}
				return contractClient{c}
			}, func(err error) objhttp.Class { return client.Classify(err) })
		})
	}
}
