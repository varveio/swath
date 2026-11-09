// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// TestBinaryLinksSDKClient: the command's default build links the AWS
// SDK's S3 service client, the one S3 client's request path.
func TestBinaryLinksSDKClient(t *testing.T) {
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
	if deps := strings.Fields(string(out)); !slices.Contains(deps, "github.com/aws/aws-sdk-go-v2/service/s3") {
		t.Fatal("cmd/swath does not link github.com/aws/aws-sdk-go-v2/service/s3")
	}
}
