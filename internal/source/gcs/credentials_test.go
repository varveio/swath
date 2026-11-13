// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
)

// openFlags opens gs://bkt with the command-line flags args, collecting
// the warnings.
func openFlags(t *testing.T, warned *[]string, args ...string) (registry.Source, error) {
	t.Helper()
	f, err := registry.Lookup("gs://bkt")
	if err != nil {
		t.Fatal(err)
	}
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	o := f.RegisterFlags(fs, false)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return open(context.Background(), registry.Request{Root: "gs://bkt", Options: o, Workers: 1, Warn: func(s string) { *warned = append(*warned, s) }})
}

// TestAnonymousCredentialConflict: --anonymous beside
// --gcs-credentials-file is refused (exit 2), as for every provider, where
// the file used to be dropped without a word.
func TestAnonymousCredentialConflict(t *testing.T) {
	var warned []string
	src, err := openFlags(t, &warned, "--anonymous", "--gcs-credentials-file", "/nonexistent.json", "--endpoint-url", "http://127.0.0.1:1/storage/v1/")
	if src != nil || !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "--anonymous excludes --gcs-credentials-file") {
		t.Fatalf("opened %v, error %v", src, err)
	}
}

// TestAnonymousNote: GOOGLE_APPLICATION_CREDENTIALS, which --anonymous
// ignores, is named in the one-line warning every provider gives.
func TestAnonymousNote(t *testing.T) {
	endpoint := gcstest.Bucket(t, []string{"a"}, 0)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent.json")
	var warned []string
	if _, err := openFlags(t, &warned, "--anonymous", "--endpoint-url", endpoint); err != nil {
		t.Fatal(err)
	}
	want := "--anonymous: ignoring the GCS credentials in the environment (GOOGLE_APPLICATION_CREDENTIALS)"
	if len(warned) != 1 || warned[0] != want {
		t.Fatalf("warnings %q, want %q", warned, want)
	}
}
