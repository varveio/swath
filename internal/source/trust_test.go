// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestTrustRefusesUnusableBundle: a CA bundle that cannot be read or holds
// no PEM certificate is refused (exit 2), never taken for the system roots.
func TestTrustRefusesUnusableBundle(t *testing.T) {
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{empty, filepath.Join(t.TempDir(), "missing.pem")} {
		if c, err := (Trust{CABundle: f}).Config(); c != nil || !errors.Is(err, ErrUnsupported) {
			t.Errorf("%s: %v, %v", f, c, err)
		}
	}
	if c, err := (Trust{}).Config(); c != nil || err != nil {
		t.Fatalf("no trust flags: %v, %v", c, err)
	}
}
