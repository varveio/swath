// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/varveio/swath/objfile"
)

// TestFailureClass: a status an adapter attached, or an SDK error that
// has an HTTPStatusCode (S3's), is "http_STATUS"; the rest are classed by
// how the attempt failed.
func TestFailureClass(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{objfile.WithHTTPStatus(errors.New("slow down"), 503), "http_503"},
		{fmt.Errorf("wrapped: %w", objfile.WithHTTPStatus(errors.New("gone"), 404)), "http_404"},
		{context.DeadlineExceeded, "timeout"},
		{&net.OpError{Op: "read", Err: errors.New("reset")}, "connection"},
		{io.ErrUnexpectedEOF, "connection"},
		{errors.New("other"), "other"},
	} {
		if got := FailureClass(c.err); got != c.want {
			t.Errorf("FailureClass(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
