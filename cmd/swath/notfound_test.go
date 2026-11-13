// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestMissingBucketCause: a bucket (S3, GCS) or container (Azure Blob) that
// does not exist ends the run fatal, exit 1, not resumable, with cause
// not_found in the report and in error.json, its message kept; a 404 that
// names an object, or nothing (a wrong endpoint path), has no cause.
func TestMissingBucketCause(t *testing.T) {
	captureStderr(t)
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	answer := func(status int, ctype, body string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", ctype)
			if i, j := strings.Index(body, "<Code>"), strings.Index(body, "</Code>"); i >= 0 && j > i {
				w.Header().Set("x-ms-error-code", body[i+6:j]) // Azure's; S3 reads the body
			}
			w.WriteHeader(status)
			io.WriteString(w, body)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	s3 := s3test.Bucket(t, s3test.Keys(10, 2)) // serves bucket bkt only: any other is NoSuchBucket
	az := azblobtest.Serve(t, []string{"a"})   // serves its container only
	azEP := strings.TrimSuffix(az.Endpoint, "/"+az.Account)
	gcsMissing := answer(404, "application/json",
		`{"error":{"code":404,"message":"The specified bucket does not exist.","errors":[{"message":"The specified bucket does not exist.","domain":"global","reason":"notFound"}]}}`)
	for _, c := range []struct {
		name    string
		args    []string
		cause   string
		message string
	}{
		{"s3 bucket", []string{"s3://nobucket/p/", "--endpoint-url", s3, "--s3-path-style", "--anonymous"}, "not_found", "NoSuchBucket"},
		{"gcs bucket", []string{"gs://nobucket/p/", "--endpoint-url", gcsMissing + gcstest.Path, "--anonymous"}, "not_found", "notFound"},
		{"azure container", []string{"az://" + az.Account + "/nocontainer/p/", "--endpoint-url", azEP, "--anonymous", "--azure-assume-flat"}, "not_found", "ContainerNotFound"},
		{"azure container, namespace read", []string{"az://" + az.Account + "/nocontainer/p/", "--endpoint-url", azEP, "--anonymous"}, "not_found", "ContainerNotFound"},
		// Not a bucket's: no cause, the status and code in the message.
		{"s3 key 404", []string{"s3://bkt/p/", "--endpoint-url", answer(404, "application/xml", `<Error><Code>NoSuchKey</Code><Message>m</Message></Error>`),
			"--s3-path-style", "--anonymous"}, "", "NoSuchKey"},
		{"s3 404 without a code", []string{"s3://bkt/p/", "--endpoint-url", answer(404, "text/plain", ``), "--s3-path-style", "--anonymous"}, "", "404"},
		{"gcs 404 without notFound", []string{"gs://bkt/p/", "--endpoint-url", answer(404, "application/json", `{"error":{"code":404,"message":"no route"}}`) + gcstest.Path,
			"--anonymous"}, "", "404"},
		{"azure blob 404", []string{"az://" + az.Account + "/c/p/", "--endpoint-url", answer(404, "application/xml", `<?xml version="1.0" encoding="utf-8"?><Error><Code>BlobNotFound</Code><Message>m</Message></Error>`),
			"--anonymous", "--azure-assume-flat"}, "", "BlobNotFound"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ds")
			report := filepath.Join(t.TempDir(), "report.json")
			args := append(append(c.args, "--workers", "1", "--report", report, "-o", dir), unjournaled...)
			err := run(context.Background(), args, io.Discard, diagList)
			if code := exitCode(t, err); code != exitFatal {
				t.Fatalf("exit %d (%v)", code, err)
			}
			var rep listing.Report
			readJSON(t, report, &rep)
			o := rep.Outcome
			if o.Class != listing.ClassFatal || o.Cause != c.cause || o.Resumable || o.ExitCode != exitFatal || !strings.Contains(o.Error, c.message) {
				t.Fatalf("report outcome %+v", o)
			}
			b, err := os.ReadFile(filepath.Join(dir, dataset.ErrorName))
			if err != nil {
				t.Fatal(err)
			}
			var e outcome
			if err := json.Unmarshal(b, &e); err != nil {
				t.Fatal(err)
			}
			if e.Class != listing.ClassFatal || e.Cause != c.cause || e.Resumable || !strings.Contains(e.Message, c.message) {
				t.Fatalf("error.json %+v", e)
			}
			if c.cause == "" && strings.Contains(string(b), "not_found") {
				t.Fatalf("error.json names not_found: %s", b)
			}
		})
	}
}

// TestRegionRedirectCause: S3 answering that the bucket is in another
// region, and naming it, ends the run fatal, exit 1, not resumable, with
// cause region_redirect and the region in the message.
func TestRegionRedirectCause(t *testing.T) {
	captureStderr(t)
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("X-Amz-Bucket-Region", "eu-west-2")
		w.WriteHeader(http.StatusMovedPermanently)
		io.WriteString(w, `<Error><Code>PermanentRedirect</Code><Message>The bucket you are attempting to access must be addressed using the specified endpoint.</Message></Error>`)
	}))
	t.Cleanup(srv.Close)
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "report.json")
	args := append([]string{"s3://bkt/p/", "--endpoint-url", srv.URL, "--s3-path-style", "--anonymous",
		"--workers", "1", "--report", report, "-o", dir}, unjournaled...)
	err := run(context.Background(), args, io.Discard, diagList)
	if code := exitCode(t, err); code != exitFatal {
		t.Fatalf("exit %d (%v)", code, err)
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	o := rep.Outcome
	if o.Class != listing.ClassFatal || o.Cause != "region_redirect" || o.Resumable || !strings.Contains(o.Error, "eu-west-2") {
		t.Fatalf("report outcome %+v", o)
	}
}
