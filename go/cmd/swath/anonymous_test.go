// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
)

// TestPublicBucketHint: a listing that fails because no credentials were
// found (swath never falls back to anonymous requests) names --anonymous,
// sends no listing request, and lists with it: S3, refused at
// construction, and Azure, whose default chain fails the first request.
func TestPublicBucketHint(t *testing.T) {
	captureStderr(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_USERNAME", "AZURE_FEDERATED_TOKEN_FILE",
		"IDENTITY_ENDPOINT", "MSI_ENDPOINT", "AZURE_STORAGE_KEY", "AZURE_STORAGE_SAS_TOKEN"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(home, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(home, "credentials"))
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "EnvironmentCredential") // the chain without the CLI logins or a metadata probe

	var s3Requests atomic.Int64
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s3Requests.Add(1) }))
	defer s3.Close()
	az := azblobtest.Serve(t, []string{"a", "b"})
	var azRequests atomic.Int64
	azProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		azRequests.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer azProxy.Close()
	for _, c := range []struct {
		name     string
		args     []string
		requests *atomic.Int64
	}{
		{"s3", []string{"s3://bkt", "--endpoint", s3.URL}, &s3Requests},
		{"azblob normal endpoint", []string{"az://replay/corpus"}, &azRequests},
		{"azblob", []string{"az://replay/corpus", "--azblob-endpoint", azProxy.URL}, &azRequests},
	} {
		out := filepath.Join(t.TempDir(), "ds")
		err := run(context.Background(), append(c.args, "-o", out), io.Discard, false)
		if err == nil || !strings.Contains(err.Error(), "(a public bucket needs --anonymous)") || c.requests.Load() != 0 {
			t.Errorf("%s: %v, %d requests; want the hint and none sent", c.name, err, c.requests.Load())
		}
		if c.name == "azblob normal endpoint" && (err == nil || !strings.Contains(err.Error(), "filesystem properties:")) {
			t.Errorf("normal endpoint did not preserve both refused namespace checks: %v", err)
		}
	}
	// The hint is right: the public container lists with --anonymous.
	out := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{"az://replay/corpus", "--azblob-endpoint", azEndpoint(az), "--anonymous", "-o", out}, io.Discard, false); err != nil {
		t.Fatalf("--anonymous: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal(err)
	}
}
