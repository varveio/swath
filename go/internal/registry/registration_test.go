// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"slices"
	"testing"
)

func TestRegisterDuplicate(t *testing.T) {
	const scheme = "duplicate-test"
	t.Cleanup(func() { regMu.Lock(); defer regMu.Unlock(); delete(factories, scheme) })
	Register(Factory{Scheme: scheme})
	defer func() {
		if got := recover(); got != "registry: scheme registered twice: "+scheme {
			t.Errorf("panic=%v", got)
		}
	}()
	Register(Factory{Scheme: scheme})
}

func TestLookupUnknownScheme(t *testing.T) {
	const root = "unknown-registry-test://bucket/prefix"
	if _, err := Lookup(root); err == nil || err.Error() != "unsupported source: no provider for unknown-registry-test://" {
		t.Fatalf("unknown scheme: %v", err)
	}
}

func TestRedactSecretArgv(t *testing.T) {
	const scheme = "secret-argv-test"
	t.Cleanup(func() { regMu.Lock(); defer regMu.Unlock(); delete(factories, scheme) })
	Register(Factory{Scheme: scheme, Secrets: []Secret{{Flag: "secret-test", Env: "SECRET_TEST"}}})
	for _, argv := range [][]string{
		{"list", "--secret-test", "password", "--other=value", scheme + "://bucket"},
		{"list", "--secret-test=password", "--other=value", scheme + "://bucket"},
	} {
		original := slices.Clone(argv)
		want := []string{"list", "--secret-test=[redacted]", "--other=value", scheme + "://bucket"}
		got := RedactArgv(argv)
		if !slices.Equal(got, want) || !slices.Equal(argv, original) || !slices.Equal(RedactArgv(got), want) {
			t.Errorf("%q: got %q, want %q; input %q", original, got, want, argv)
		}
	}
}
