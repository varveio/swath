// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestResumeOperationalOverrides: swath resume accepts the operational
// flags no identity holds (Java let credentials change across a resume):
// a run interrupted by its deadline under one profile resumes under
// another on the same route, signs with the new profile's keys and
// completes; the source stays frozen (a different prefix is refused), and
// so does the route (a profile that resolves another region is refused by
// the identity check).
func TestResumeOperationalOverrides(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	dir0 := t.TempDir()
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir0, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir0, "credentials"))
	os.WriteFile(filepath.Join(dir0, "credentials"), []byte("[a]\naws_access_key_id = AKA\naws_secret_access_key = s\n"+
		"[b]\naws_access_key_id = AKB\naws_secret_access_key = s\n[c]\naws_access_key_id = AKC\naws_secret_access_key = s\n"), 0o600)
	os.WriteFile(filepath.Join(dir0, "config"), []byte("[profile c]\nregion = eu-west-2\n"), 0o600)
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(20000, 7)))
	var mu sync.Mutex
	keys := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a := r.Header.Get("Authorization")
		if i := strings.Index(a, "Credential="); i >= 0 {
			mu.Lock()
			keys[strings.SplitN(a[i+len("Credential="):], "/", 2)[0]]++
			mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "ds")
	args := []string{"s3://bkt/p0", "--endpoint-url", srv.URL, "--s3-path-style", "--s3-profile", "a", "--workers", "2", "--part-size", "4KiB", "-o", dir}
	if c := exitCode(t, run(ctx, append(args, "--max-duration", "300ms"), io.Discard, false)); c != exitDeadline {
		t.Fatalf("first attempt: exit %d", c)
	}
	// The source is frozen: another prefix is not an override.
	var u usageError
	if err := resume(ctx, []string{dir, "s3://bkt/p1"}, io.Discard); !errors.As(err, &u) {
		t.Fatalf("a changed prefix: %v, want a usage refusal", err)
	}
	// The route is frozen: profile c resolves eu-west-2, another endpoint
	// region than the recorded run's.
	if err := resume(ctx, []string{dir, "--s3-profile", "c", "--max-duration", "0"}, io.Discard); err == nil || !strings.Contains(err.Error(), "cannot resume") {
		t.Fatalf("a profile that moves the route: %v, want an identity refusal", err)
	}
	mu.Lock()
	clear(keys)
	mu.Unlock()
	if err := resume(ctx, []string{dir, "--s3-profile", "b", "--request-timeout", "10s", "--no-adaptive-limit", "--request-rate", "1000",
		"--max-duration", "0"}, io.Discard); err != nil {
		t.Fatalf("resume with another profile: %v", err)
	}
	if !dataset.Committed(dir) {
		t.Fatal("not committed")
	}
	mu.Lock()
	defer mu.Unlock()
	if keys["AKB"] == 0 || keys["AKA"] != 0 {
		t.Fatalf("the resume signed with %v, want profile b's key only", keys)
	}
}
