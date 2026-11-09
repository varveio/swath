// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3test serves an in-process S3 bucket (gofakes3) for tests (test
// support; nothing in the tool imports it): bucket population, the
// server's lifecycle and isolation from the host's AWS credentials. A test
// that injects faults wraps Handler before serving it; protocol oracles
// and malformed-response handlers stay with their tests.
package s3test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// Name is the bucket every fixture serves.
const Name = "bkt"

// Keys are n names in dirs folders, "p00/obj-000000" onwards.
func Keys(n, dirs int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("p%02d/obj-%06d", i%dirs, i)
	}
	return keys
}

// Backend is bucket Name holding keys, each object's body its key, so
// sizes and ETags differ per object; opts are s3mem's (a time source).
func Backend(t testing.TB, keys []string, opts ...s3mem.Option) *s3mem.Backend {
	t.Helper()
	b := s3mem.New(opts...)
	if err := b.CreateBucket(Name); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if _, err := b.PutObject(Name, k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// Handler serves b's S3 API.
func Handler(b *s3mem.Backend) http.Handler { return gofakes3.New(b).Server() }

// Delayed answers through h after waiting d.
func Delayed(h http.Handler, d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(d)
		h.ServeHTTP(w, r)
	})
}

// Serve serves h until the test ends and returns its URL.
func Serve(t testing.TB, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// Bucket serves bucket Name holding keys and returns its endpoint.
func Bucket(t testing.TB, keys []string) string {
	t.Helper()
	return Serve(t, Handler(Backend(t, keys)))
}

// NoCredentials hides the host's AWS credentials, profiles and instance
// metadata from the test: a run then lists only with Anonymous (or the
// credentials it sets itself), and fails with auth otherwise.
func NoCredentials(t testing.TB) {
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
}
