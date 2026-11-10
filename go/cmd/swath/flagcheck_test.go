// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNegativeDurationsRefused (R9): every duration flag refuses a negative
// value with exit 2 before the source is opened; 0 still disables
// --max-duration and --stall-timeout.
func TestNegativeDurationsRefused(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{src, "--max-duration=-1s"},
		{src, "--stall-timeout=-1s"},
		{src, "--progress-interval=-1s"},
		{src, "--part-interval=-1s"},
		{"s3://bucket", "--request-timeout=-1s"},
		{"gs://bucket", "--request-timeout=-1s"},
		{"az://acct/c", "--request-timeout=-1s"},
	} {
		var buf bytes.Buffer
		err := run(context.Background(), args, &buf, false)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "negative") {
			t.Errorf("%v: exit %d (%v), want %d", args, exitOf(err), err, exitUsage)
		}
	}
	var buf bytes.Buffer
	if err := runCount(context.Background(), []string{src, "--max-duration=0", "--stall-timeout=0", "--no-stats"}, &buf); err != nil {
		t.Fatalf("zero durations: %v", err)
	}
}

// TestForeignProviderFlagsRefused (R10): a flag of a provider other than
// the root's is refused with exit 2, not ignored; the root's own flags and
// the shared groups' flags (the store's, the engine's) are accepted for
// every object store, and refused for a local root.
func TestForeignProviderFlagsRefused(t *testing.T) {
	src := t.TempDir()
	for _, tc := range []struct {
		args []string
		flag string
	}{
		{[]string{"gs://bucket", "--s3-region", "eu-west-1"}, "--s3-region"},
		{[]string{"az://acct/c", "--s3-path-style"}, "--s3-path-style"},
		{[]string{"s3://bucket", "--gcs-billing-project", "p"}, "--gcs-billing-project"},
		{[]string{src, "--endpoint-url", "http://127.0.0.1:1"}, "--endpoint-url"},
		{[]string{src, "--request-timeout", "5s"}, "--request-timeout"},
		{[]string{"s3://bucket", "--gcs-credentials-file", "f.json"}, "--gcs-credentials-file"},
		{[]string{"gs://bucket", "--azure-sas-token", "sv=1&sig=x"}, "--azure-sas-token"},
		{[]string{src, "--anonymous"}, "--anonymous"},
		{[]string{src, "--s3-region", "eu-west-1"}, "--s3-region"},
		{[]string{src, "--request-rate", "10"}, "--request-rate"},
	} {
		var buf bytes.Buffer
		err := run(context.Background(), tc.args, &buf, false)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), tc.flag+" applies to") {
			t.Errorf("%v: exit %d (%v), want %d naming %s", tc.args, exitOf(err), err, exitUsage, tc.flag)
		}
	}
	// The flag set knows each provider flag's owners.
	var o options
	newFlags(&o, false)
	for name, want := range map[string]string{"anonymous": "az,gs,s3", "endpoint-url": "az,gs,s3", "request-timeout": "az,gs,s3", "s3-profile": "s3", "gcs-credentials-file": "gs", "azure-sas-token": "az", "request-rate": "az,gs,s3"} {
		if got := strings.Join(o.owners[name], ","); got != want {
			t.Errorf("owners of --%s: %q, want %q", name, got, want)
		}
	}
	for _, root := range []string{"s3://bucket", "gs://bucket", "az://acct/c"} {
		if _, _, err := parseList([]string{root, "--endpoint-url", "http://127.0.0.1:1", "--request-timeout", "5s", "--anonymous", "--request-rate", "10"}, io.Discard, command{}); err != nil {
			t.Errorf("%s with the shared flags: %v", root, err)
		}
	}
	if _, ok := o.owners["workers"]; ok {
		t.Error("--workers, a command flag, has a provider owner")
	}
	var buf bytes.Buffer
	if err := runCount(context.Background(), []string{src, "--no-stats", "--workers", "2"}, &buf); err != nil {
		t.Fatalf("a command flag with a local root: %v", err)
	}
}

// TestLocalTextRefusedUpFront: a local source's keys are bytes, so a text
// run of one (no -o, -o -, or a text file) is refused with exit 2 before
// the walk, naming the alternatives.
func TestLocalTextRefusedUpFront(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{src}, {src, "-o", "-"}, {src, "-o", filepath.Join(t.TempDir(), "rows.tsv")}} {
		var buf bytes.Buffer
		err := run(context.Background(), args, &buf, false)
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "needs -o DIR (a dataset), or swath du") {
			t.Errorf("%v: exit %d (%v)", args, exitOf(err), err)
		}
		if buf.Len() != 0 {
			t.Errorf("%v: wrote %q", args, buf.String())
		}
	}
}
