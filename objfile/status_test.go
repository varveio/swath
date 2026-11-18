// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile_test

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"github.com/varveio/swath/objfile"
)

// TestWithHTTPStatus: the status is reachable by HTTPStatusCode, the text
// and the chain are the error's own, and an error without an answer is
// returned as it was.
func TestWithHTTPStatus(t *testing.T) {
	inner := fmt.Errorf("%w: slow down", fs.ErrPermission)
	err := objfile.WithHTTPStatus(inner, 503)
	var re interface{ HTTPStatusCode() int }
	if !errors.As(err, &re) || re.HTTPStatusCode() != 503 {
		t.Fatalf("status not reachable in %v", err)
	}
	if err.Error() != inner.Error() || !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("text or chain changed: %q", err)
	}
	if got := objfile.WithHTTPStatus(inner, 0); got != inner {
		t.Fatalf("status 0 wrapped: %v", got)
	}
	if objfile.WithHTTPStatus(nil, 500) != nil {
		t.Fatal("nil wrapped")
	}
}
