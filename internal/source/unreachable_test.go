// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
)

// TestUnreachable: a real refused connection and a host not found are
// deterministic connect failures; a temporary DNS failure, a timeout, a
// cut connection and a refused certificate are not.
func TestUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := ln.Addr().String()
	ln.Close()
	_, refused := http.Get("http://" + host + "/") // the platform's own error, wrapped as a client sees it
	if refused == nil {
		t.Skip("a closed port answered")
	}
	dial := func(err error) error {
		return &url.Error{Op: "Get", URL: "http://h/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: err}}
	}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"refused", refused, true},
		{"host not found", dial(&net.DNSError{Err: "no such host", Name: "nohost.invalid", IsNotFound: true}), true},
		{"dns timeout", dial(&net.DNSError{Err: "i/o timeout", Name: "h", IsTimeout: true}), false},
		{"dns temporary", dial(&net.DNSError{Err: "server misbehaving", Name: "h", IsTemporary: true}), false},
		{"deadline", dial(context.DeadlineExceeded), false},
		{"cut", dial(io.ErrUnexpectedEOF), false},
		{"certificate", dial(&tls.CertificateVerificationError{Err: errors.New("x509: unknown authority")}), false},
		{"nil", nil, false},
	} {
		if got := Unreachable(c.err); got != c.want {
			t.Errorf("%s: Unreachable(%v) = %v", c.name, c.err, got)
		}
	}
}
