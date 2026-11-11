// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile_test

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"
	"testing"

	"github.com/varveio/swath/go/objfile"
)

// TestRedactError: every URL in an error's text loses its query, user
// information and fragment; the chain under it is kept for errors.Is and
// errors.As.
func TestRedactError(t *testing.T) {
	inner := &url.Error{Op: "Post", URL: "https://storage.googleapis.com/upload/storage/v1/b/b/o?uploadType=resumable&upload_id=SECRET1",
		Err: fmt.Errorf("%w: reset", fs.ErrClosed)}
	err := objfile.RedactError(fmt.Errorf("%w: %w", objfile.ErrUnresolved, inner))
	for _, secret := range []string{"SECRET1", "upload_id"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("%q is in %q", secret, err)
		}
	}
	if !strings.Contains(err.Error(), "https://storage.googleapis.com/upload/storage/v1/b/b/o?[redacted]") {
		t.Fatalf("the URL's place is lost: %q", err)
	}
	var ue *url.Error
	if !errors.Is(err, objfile.ErrUnresolved) || !errors.Is(err, fs.ErrClosed) || !errors.As(err, &ue) {
		t.Fatal("the chain is not kept")
	}
	for in, want := range map[string]string{
		"get http://user:pw@h:1/p#frag":  "get http://h:1/p?[redacted]",
		"a s3://b/k?X-Amz-Signature=S x": "a s3://b/k?[redacted] x",
		"no url here":                    "no url here",
		`Post "http://h/p": EOF`:         `Post "http://h/p": EOF`,
	} {
		if got := objfile.RedactText(in); got != want {
			t.Errorf("RedactText(%q) = %q, want %q", in, got, want)
		}
	}
	if objfile.RedactError(nil) != nil {
		t.Fatal("nil is not nil")
	}
}
