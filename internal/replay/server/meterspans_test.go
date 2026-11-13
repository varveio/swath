// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"testing"

	"github.com/varveio/swath/internal/replay/metrics"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

// TestRequestMeterSpans pins where the request meters end on the S3 and
// GCS routes: http.request.latency times every answer up to its write, and
// the write stage is a page's only, never an error's.
func TestRequestMeterSpans(t *testing.T) {
	s := start(t, gcsConfig(memstore.Keys(memstore.Options{}, "a", "b"), ProtocolS3, ProtocolGCS))
	for _, target := range []string{"/bucket?list-type=2", "/nosuch?list-type=2", "/bucket?list-type=2&max-keys=x",
		gcsList, "/storage/v1/b/nosuch/o"} {
		get(t, s, target)
	}
	if n := count(s, metrics.RequestStageLatency, "s3", "write"); n != 1 {
		t.Errorf("s3 write stage %d, want the one page", n)
	}
	if n := count(s, metrics.RequestStageLatency, "gcs", "write"); n != 1 {
		t.Errorf("gcs write stage %d, want the one page", n)
	}
	if count(s, metrics.HTTPRequestLatency) != 5 || count(s, metrics.HTTPRequests) != 5 {
		t.Errorf("request latency %d, requests %d", count(s, metrics.HTTPRequestLatency), count(s, metrics.HTTPRequests))
	}
}

// TestAzureErrorHasNoWriteStage: on the Azure route too only a page
// records the write stage.
func TestAzureErrorHasNoWriteStage(t *testing.T) {
	s := start(t, azureConfig("s3", "azure"))
	azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list", "x-ms-version", "2026-06-06")
	azureDo(t, s, "GET", "/replay/other?restype=container&comp=list", "x-ms-version", "2026-06-06")
	if n := count(s, metrics.RequestStageLatency, "azure", "write"); n != 1 {
		t.Errorf("azure write stage %d, want the one page", n)
	}
}
