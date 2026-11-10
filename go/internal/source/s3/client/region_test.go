// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"strings"
	"testing"
)

// TestWrongRegionNamesTheFix: an answer that names the bucket's region
// says which flag to give.
func TestWrongRegionNamesTheFix(t *testing.T) {
	e := &Error{Status: 301, Code: "PermanentRedirect", Region: "eu-west-1", RequestID: "RID"}
	if s := e.Error(); !strings.Contains(s, "(the bucket is in eu-west-1: give --s3-region eu-west-1)") {
		t.Fatal(s)
	}
}
