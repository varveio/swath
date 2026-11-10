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
	_, err := c.Client.List(ctx, &ListInput{Container: "c"})
	return err
}

// TestRetryContract: the Azure client passes the shared retry and
// classification contract with its own XML answers.
func TestRetryContract(t *testing.T) {
	wire := objhttptest.Wire{
		OK:       func(w http.ResponseWriter) { fmt.Fprint(w, page("")) },
		Throttle: func(w http.ResponseWriter) { azureError(w, 503, codeServerBusy) },
		Server:   func(w http.ResponseWriter) { azureError(w, 500, codeInternalError) },
		Denied:   func(w http.ResponseWriter) { azureError(w, 403, codeAuthorization) },
		Garbled:  func(w http.ResponseWriter) { fmt.Fprint(w, listHead+"<Blob><Na") },
	}
	objhttptest.Contract(t, wire, func(s objhttptest.Settings) objhttptest.Client {
		c, err := New(context.Background(), Config{Endpoint: s.Endpoint, Account: "acct", Anonymous: true, RequestTimeout: s.Timeout, HTTPClient: s.HTTPClient,
			Retry: objhttp.Retry{MaxAttempts: s.Retry.MaxAttempts, BaseBackoff: s.Retry.BaseBackoff, MaxBackoff: s.Retry.MaxBackoff, Limiter: s.Retry.Limiter}})
		if err != nil {
			t.Fatal(err)
		}
		return contractClient{c}
	})
}
