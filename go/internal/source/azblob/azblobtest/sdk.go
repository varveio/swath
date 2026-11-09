// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"net/http"
	"strings"

	"github.com/varveio/swath/go/internal/replay/azure"
)

// SDKVersion is the x-ms-version PinVersion sends: one of the captured
// service versions the replay's Azure route answers.
const SDKVersion = azure.Version20261006

// PinVersion is a transport for the Azure SDK (an azcore
// policy.Transporter: set it as azcore.ClientOptions.Transport) that sends
// every request with x-ms-version SDKVersion. It exists because the replay
// answers only the service versions it has captures for, on purpose, and
// the pinned azblob release sends a newer one (2026-04-06) with no option
// to change it; SDK-oracle tests against Serve need the override. The SDK
// keeps its x-ms-* header names in lower case, outside the canonical key,
// so every spelling is replaced. Next carries the request
// (http.DefaultTransport when nil).
type PinVersion struct {
	Next http.RoundTripper
}

// Do sends req with the pinned version.
func (p PinVersion) Do(req *http.Request) (*http.Response, error) {
	for k := range req.Header {
		if strings.EqualFold(k, "x-ms-version") {
			delete(req.Header, k)
		}
	}
	req.Header["x-ms-version"] = []string{SDKVersion}
	next := p.Next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(req)
}
