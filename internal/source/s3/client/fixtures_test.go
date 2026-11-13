// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestValidateFailuresAreInvalidPage: a truncated versions page without a
// NextKeyMarker, or ending at it without a version-id marker, is
// invalid_page after one attempt, not retried.
func TestValidateFailuresAreInvalidPage(t *testing.T) {
	s3test.NoCredentials(t)
	for _, body := range []string{
		`<ListVersionsResult><IsTruncated>true</IsTruncated><Version><Key>k</Key><VersionId>v</VersionId></Version></ListVersionsResult>`,
		`<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>k</NextKeyMarker><Version><Key>k</Key><VersionId>v</VersionId></Version></ListVersionsResult>`,
	} {
		c := newTest(t, testConfig(s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))))
		_, err := c.ListObjectVersions(context.Background(), &VersionsInput{Bucket: "b"})
		if !errors.Is(err, objhttp.ErrInvalidPage) || c.Counters().Attempts != 1 || c.Counters().ParseFailures != 0 {
			t.Fatalf("%v (%q), %+v", err, objhttp.Classify(err), c.Counters())
		}
	}
}
