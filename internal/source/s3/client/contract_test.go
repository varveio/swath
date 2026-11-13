// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/objhttp/objhttptest"
)

type contractClient struct{ *Client }

func (c contractClient) List(ctx context.Context) error {
	_, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "b"})
	return err
}

// TestRetryContract: the S3 client passes the shared retry and
// classification contract with its own XML answers.
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
	objhttptest.Contract(t, wire, func(s objhttptest.Settings) objhttptest.Client {
		c, err := New(context.Background(), Config{Endpoint: s.Endpoint, PathStyle: true, Anonymous: true, Region: "us-east-1", RequestTimeout: s.Timeout, HTTPClient: s.HTTPClient,
			Retry: objhttp.Retry{MaxAttempts: s.Retry.MaxAttempts, BaseBackoff: s.Retry.BaseBackoff, MaxBackoff: s.Retry.MaxBackoff, Limiter: s.Retry.Limiter}})
		if err != nil {
			t.Fatal(err)
		}
		return contractClient{c}
	})
}
