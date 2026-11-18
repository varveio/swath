// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/gcs"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/keyrange"
)

// TestGCSAnonymousCredentialConflict: --anonymous beside
// --gcs-credentials-file is refused (exit 2), as for every provider, where
// the file used to be dropped without a word.
func TestGCSAnonymousCredentialConflict(t *testing.T) {
	src, err := openArgs(t, "gs://bkt", false, nil, "--anonymous", "--gcs-credentials-file", "/nonexistent.json", "--endpoint-url", "http://127.0.0.1:1/storage/v1/")
	if src != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "--anonymous excludes --gcs-credentials-file") {
		t.Fatalf("opened %v, error %v", src, err)
	}
}

// TestGCSAnonymousNote: GOOGLE_APPLICATION_CREDENTIALS, which --anonymous
// ignores, is named in the one-line warning every provider gives.
func TestGCSAnonymousNote(t *testing.T) {
	endpoint := gcstest.Bucket(t, []string{"a"}, 0)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent.json")
	var warned []string
	if _, err := openArgs(t, "gs://bkt", false, func(_, s string) { warned = append(warned, s) }, "--anonymous", "--endpoint-url", endpoint); err != nil {
		t.Fatal(err)
	}
	want := "--anonymous: ignoring the GCS credentials in the environment (GOOGLE_APPLICATION_CREDENTIALS)"
	if len(warned) != 1 || warned[0] != want {
		t.Fatalf("warnings %q, want %q", warned, want)
	}
}

// TestGCSEngineFlagsShared: GCS takes the store's and the range engine's
// flag groups the command registered once and passed to it, and opens
// with the parsed values.
func TestGCSEngineFlagsShared(t *testing.T) {
	f := factory(t, "gs://bkt")
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	v := f.RegisterFlags(fs, true)
	if err := fs.Parse([]string{"--structure", "--anonymous", "--endpoint-url", "http://127.0.0.1:1/storage/v1/", "--request-timeout", "7s"}); err != nil {
		t.Fatal(err)
	}
	src, err := f.Open(context.Background(), registry.Request{Root: "gs://bkt", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	opt := src.(*gcsSource[gcs.Object]).Options()
	if !opt.Seed || opt.Structure != keyrange.StructureOn || !opt.Anonymous || opt.Endpoint != "http://127.0.0.1:1/storage/v1/" || opt.Timeout.Seconds() != 7 {
		t.Fatalf("shared flags not applied: %+v", opt)
	}
}
