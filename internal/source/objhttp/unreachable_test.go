// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"testing"
)

// TestCertificateFinal: a refused certificate is final at once, never
// retried, so the unreachable fail-fast needs no grace for it.
func TestCertificateFinal(t *testing.T) {
	err := &url.Error{Op: "Get", URL: "https://h/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: &tls.CertificateVerificationError{Err: errors.New("x509: unknown authority")}}}
	if f := Normalize(err); f.Transient {
		t.Fatal("a refused certificate is retried")
	}
}
