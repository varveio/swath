// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

import "testing"

func TestNonEmpty(t *testing.T) {
	type payload struct{ value string }
	calls := 0
	get := NonEmpty(func(p *payload) string {
		calls++
		return p.value
	})
	for _, value := range []string{"", "etag", " ", "\x00\xff"} {
		before := calls
		got, ok := get(&payload{value})
		if got != value || ok != (value != "") || calls != before+1 {
			t.Fatalf("%q: (%q, %v), %d calls", value, got, ok, calls-before)
		}
	}
}
