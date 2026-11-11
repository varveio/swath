// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import "testing"

// TestObjectKeepsHTMLCharacters: metadata and tags are stored as JSON text
// with &, < and > as written, so a query comparing the text finds them.
func TestObjectKeepsHTMLCharacters(t *testing.T) {
	v := "https://example.com/?a=1&b=<2>"
	if got, want := object(map[string]*string{"u": &v}), `{"u":"https://example.com/?a=1&b=<2>"}`; got != want {
		t.Fatalf("%s, want %s", got, want)
	}
}
