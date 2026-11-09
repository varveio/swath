// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/s3/client"
)

// TestSDKClientOnlyWithTag: the command links the AWS SDK's S3 service
// client (and the SDK-backed client) only when built with the
// swath_sdkclient tag.
func TestSDKClientOnlyWithTag(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	for _, tagged := range []bool{false, true} {
		args := []string{"list", "-deps", "-f", "{{.ImportPath}}"}
		if tagged {
			args = append(args, "-tags", "swath_sdkclient")
		}
		out, err := exec.Command("go", append(args, "github.com/varveio/swath/go/cmd/swath")...).Output()
		if err != nil {
			t.Fatalf("go %s: %v", strings.Join(args, " "), err)
		}
		deps := strings.Fields(string(out))
		for _, p := range []string{"github.com/aws/aws-sdk-go-v2/service/s3", "github.com/varveio/swath/go/internal/source/s3/sdkclient"} {
			if slices.Contains(deps, p) != tagged {
				t.Errorf("tagged %v: %s linked %v", tagged, p, !tagged)
			}
		}
	}
}

// TestStatsNameTheClient: Stats.Client names the built client, and is
// empty for a supplied one.
func TestStatsNameTheClient(t *testing.T) {
	url := fakeBucket(t, []string{"a", "b"}, 0)
	if _, st := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Workers: 1}, 1); st.Client != clientKind {
		t.Fatalf("built: %q, want %q", st.Client, clientKind)
	}
	c, err := newClient(t.Context(), client.Config{Endpoint: url, PathStyle: true, Anonymous: true, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, st := list(t, Options{Bucket: "bkt", Client: c, Workers: 1}, 1); st.Client != "" {
		t.Fatalf("supplied: %q", st.Client)
	}
}
