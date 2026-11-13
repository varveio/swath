// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/objhttp/objhttptest"
)

type contractClient struct{ *Client }

func (c contractClient) List(ctx context.Context) error {
	_, err := c.Client.List(ctx, &ListInput{Bucket: "b"})
	return err
}

// TestRetryContract: the GCS client passes the shared retry and
// classification contract with its own JSON answers.
func TestRetryContract(t *testing.T) {
	jsonError := func(status int, reason string) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.WriteHeader(status)
			fmt.Fprintf(w, `{"error":{"code":%d,"message":"m","errors":[{"reason":%q}]}}`, status, reason)
		}
	}
	wire := objhttptest.Wire{
		OK: func(w http.ResponseWriter) {
			fmt.Fprint(w, `{"kind":"storage#objects","items":[{"name":"a","size":"1"}]}`)
		},
		Throttle: jsonError(429, "rateLimitExceeded"),
		Server:   jsonError(503, "backendError"),
		Denied:   jsonError(403, "forbidden"),
		Garbled:  func(w http.ResponseWriter) { fmt.Fprint(w, `{"kind":"storage#objects","items":[{"na`) },
	}
	objhttptest.Contract(t, wire, func(s objhttptest.Settings) objhttptest.Client {
		c, err := New(context.Background(), Config{Endpoint: s.Endpoint + "/storage/v1/", Anonymous: true, RequestTimeout: s.Timeout, HTTPClient: s.HTTPClient,
			Retry: objhttp.Retry{MaxAttempts: s.Retry.MaxAttempts, BaseBackoff: s.Retry.BaseBackoff, MaxBackoff: s.Retry.MaxBackoff, Limiter: s.Retry.Limiter}})
		if err != nil {
			t.Fatal(err)
		}
		return contractClient{c}
	})
}
