// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNoCloudSDK: the packages ordering builds on name no cloud SDK module
// in their transitive imports (tests aside), so a build that orders local
// output links none through them. A store's errors reach a report already
// classified (dataset.FailureClass reads an HTTPStatusCode, not an SDK's
// error type).
func TestNoCloudSDK(t *testing.T) {
	sdk := []string{"github.com/aws/", "github.com/Azure/", "cloud.google.com/", "google.golang.org/api", "google.golang.org/grpc"}
	for _, pkg := range []string{"github.com/varveio/swath/internal/order", "github.com/varveio/swath/internal/dataset"} {
		out, err := exec.Command("go", "list", "-deps", "-f", "{{with .Module}}{{.Path}}{{end}}", pkg).Output()
		if err != nil {
			t.Fatalf("go list -deps %s: %v", pkg, err)
		}
		for line := range strings.Lines(string(out)) {
			m := strings.TrimSpace(line)
			for _, p := range sdk {
				if strings.HasPrefix(m, p) {
					t.Errorf("%s reaches %s", pkg, m)
				}
			}
		}
	}
}
