// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import "testing"

func TestRedactRoot(t *testing.T) {
	for _, c := range []struct{ root, want string }{
		{"s3://bucket/prefix", "s3://bucket/prefix"},
		{"s3://bucket/p?literal#key@name", "s3://bucket/p?literal#key@name"},
		{"gs://user:SECRET@bucket/p", "gs://[redacted]"},
		{"gs://bucket?sig=SECRET/p", "gs://[redacted]"},
		{"az://account#SECRET/c", "az://[redacted]"},
		{"unknown://bucket/p", "unknown://bucket/p"},
		{"/local/path", "/local/path"},
	} {
		if got := RedactRoot(c.root); got != c.want {
			t.Errorf("RedactRoot: %q, want %q", got, c.want)
		}
	}
}
