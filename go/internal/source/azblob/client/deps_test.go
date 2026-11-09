// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestBinaryLinksNoAzblob: the swath command does not link the Azure SDK's
// blob client, the test-only oracle (this client takes only azcore's
// credential types and azidentity from the SDK).
func TestBinaryLinksNoAzblob(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go list")
	}
	gobin := filepath.Join(runtime.GOROOT(), "bin", "go")
	out, err := exec.Command(gobin, "list", "-deps", "../../../../cmd/swath").CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	deps := strings.Fields(string(out))
	if len(deps) < 10 {
		t.Fatalf("go list -deps: %q", deps)
	}
	for _, dep := range deps {
		if strings.Contains(dep, "sdk/storage/azblob") {
			t.Fatalf("cmd/swath depends on %s", dep)
		}
	}
}
