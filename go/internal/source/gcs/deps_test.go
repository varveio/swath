// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// TestSDKClientBuildTag: the command links Google's generated storage
// client, and the SDK client over it, only under the swath_sdkclient build
// tag.
func TestSDKClientBuildTag(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	for _, tagged := range []bool{false, true} {
		// -tags on the command line, so a GOFLAGS tag in the environment
		// does not reach the untagged arm.
		args := []string{"list", "-deps", "-f", "{{.ImportPath}}", "-tags="}
		if tagged {
			args[len(args)-1] = "-tags=swath_sdkclient"
		}
		cmd := exec.Command("go", append(args, "github.com/varveio/swath/go/cmd/swath")...)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go %s: %v", strings.Join(args, " "), err)
		}
		deps := strings.Fields(string(out))
		for _, pkg := range []string{"google.golang.org/api/storage/v1", "github.com/varveio/swath/go/internal/source/gcs/sdkclient"} {
			if slices.Contains(deps, pkg) != tagged {
				t.Errorf("tagged %v: %s linked %v", tagged, pkg, !tagged)
			}
		}
	}
}
