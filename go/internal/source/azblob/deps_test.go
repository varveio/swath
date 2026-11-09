// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// TestSDKClientBuildTag: the command links the Azure SDK's blob service
// and container clients, and the SDK client over them, only under the
// swath_sdkclient build tag (azcore and azidentity are linked either way,
// for Entra ID).
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
		out, err := exec.Command("go", append(args, "github.com/varveio/swath/go/cmd/swath")...).Output()
		if err != nil {
			t.Fatalf("go %s: %v", strings.Join(args, " "), err)
		}
		deps := strings.Fields(string(out))
		for _, pkg := range []string{"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container",
			"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service",
			"github.com/varveio/swath/go/internal/source/azblob/sdkclient"} {
			if slices.Contains(deps, pkg) != tagged {
				t.Errorf("tagged %v: %s linked %v", tagged, pkg, !tagged)
			}
		}
		if !slices.Contains(deps, "github.com/Azure/azure-sdk-for-go/sdk/azidentity") {
			t.Errorf("tagged %v: azidentity not linked", tagged)
		}
	}
}
