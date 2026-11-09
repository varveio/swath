// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// TestBinaryLinksGeneratedStorageClient: the command lists GCS through
// Google's generated JSON API client, so the default build links
// google.golang.org/api/storage/v1.
func TestBinaryLinksGeneratedStorageClient(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	// -tags on the command line, so a GOFLAGS tag in the environment does
	// not change the build.
	args := []string{"list", "-deps", "-f", "{{.ImportPath}}", "-tags=", "github.com/varveio/swath/go/cmd/swath"}
	out, err := exec.Command("go", args...).Output()
	if err != nil {
		t.Fatalf("go %s: %v", strings.Join(args, " "), err)
	}
	if !slices.Contains(strings.Fields(string(out)), "google.golang.org/api/storage/v1") {
		t.Error("cmd/swath does not link google.golang.org/api/storage/v1")
	}
}
