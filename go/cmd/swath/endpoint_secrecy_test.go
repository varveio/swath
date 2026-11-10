// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// Endpoint userinfo is refused, and must also be removed from the recorded
// argv. A successful run can receive old recorded arguments through runWith
// (resume); those need the same protection as a new CLI failure.
func TestEndpointUserinfoReports(t *testing.T) {
	captureStderr(t)
	s3 := httptest.NewServer(s3test.Handler(s3test.Backend(t, []string{"a", "b"})))
	defer s3.Close()
	az := azblobtest.Serve(t, []string{"a", "b"})
	for _, c := range []struct{ provider, root, endpoint string }{
		{"s3", "s3://bkt", s3.URL},
		{"gcs", "gs://bkt", gcstest.Bucket(t, []string{"a", "b"}, 0)},
		{"azblob", "az://replay/corpus", azEndpoint(az)},
	} {
		for _, eq := range []bool{false, true} {
			for _, success := range []bool{false, true} {
				t.Run(c.provider+"/equals="+strconv.FormatBool(eq)+"/success="+strconv.FormatBool(success), func(t *testing.T) {
					u, err := url.Parse(c.endpoint)
					if err != nil {
						t.Fatal(err)
					}
					u.User = url.UserPassword("PRIVATE_USER", "PRIVATE_PASSWORD")
					flagArgs := func(ep string) []string {
						if eq {
							return []string{"--endpoint=" + ep}
						}
						return []string{"--endpoint", ep}
					}
					dir := filepath.Join(t.TempDir(), "ds")
					report := filepath.Join(t.TempDir(), "report.json")
					base := append([]string{c.root, "--anonymous", "-o", dir, "--report", report}, unjournaled...)
					args := append(slices.Clone(base), flagArgs(u.String())...)
					var out bytes.Buffer
					if success {
						// Current options are clean; the recorded command may predate
						// the refusal. Listing must sanitize its own copy as well.
						err = runWith(context.Background(), command{diag: diagList}, append(slices.Clone(base), flagArgs(c.endpoint)...), &out, append([]string{"list"}, args...), false, nil)
					} else {
						err = run(context.Background(), args, io.Discard, diagList)
					}
					if (err == nil) != success {
						t.Fatalf("success=%v: %v", success, err)
					}
					var rep listing.Report
					readJSON(t, report, &rep)
					if rep.Invocation.Argv == nil {
						t.Fatal("report lacks recorded arguments")
					}
					paths := []string{report}
					if success {
						paths = append(paths, filepath.Join(dir, dataset.ReportName))
					}
					for _, path := range paths {
						raw, err := os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						if bytes.Contains(raw, []byte("PRIVATE_PASSWORD")) || bytes.Contains(raw, []byte("PRIVATE_USER")) {
							t.Fatalf("endpoint userinfo in %s: argv=%q outcome=%+v", path, rep.Invocation.Argv, rep.Outcome)
						}
					}
				})
			}
		}
	}
}

func TestEndpointQuerySecrecy(t *testing.T) {
	// The escaped form is the one an SDK decodes back into a query and
	// quotes in its own error.
	for _, endpoint := range []string{"https://127.0.0.1:1/?sig=SECRET", "https://127.0.0.1:1/%3Fsig=SECRET%0A"} {
		for _, root := range []string{"s3://bkt", "gs://bkt", "az://acct/ctr"} {
			for _, eq := range []bool{false, true} {
				t.Run(root+"/equals="+strconv.FormatBool(eq)+"/escaped="+strconv.FormatBool(strings.Contains(endpoint, "%")), func(t *testing.T) {
					stderr := captureStderr(t)
					dir := filepath.Join(t.TempDir(), "ds")
					report := filepath.Join(t.TempDir(), "report.json")
					args := append([]string{root, "--anonymous", "-o", dir, "--report", report}, unjournaled...)
					if eq {
						args = append(args, "--endpoint="+endpoint)
					} else {
						args = append(args, "--endpoint", endpoint)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
					defer cancel()
					var stdout bytes.Buffer
					if code := exitCode(t, run(ctx, args, &stdout, diagList)); code != exitUsage {
						t.Errorf("exit %d, want usage %d", code, exitUsage)
					}
					check := func(where string, raw []byte) {
						t.Helper()
						if bytes.Contains(raw, []byte("SECRET")) {
							t.Errorf("credential in %s: %s", where, raw)
						}
					}
					check("stderr", stderr.Bytes())
					check("stdout", stdout.Bytes())
					raw, err := os.ReadFile(report)
					if err != nil {
						t.Fatal(err)
					}
					check("report", raw)
					if _, err := os.Stat(dir); os.IsNotExist(err) {
						return
					}
					if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
						if err != nil {
							return err
						}
						check("output path", []byte(path))
						if !d.IsDir() {
							raw, err := os.ReadFile(path)
							if err != nil {
								return err
							}
							check(path, raw)
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestShowEndpointSecrecy(t *testing.T) {
	for _, ep := range []string{"https://host/?sig=SECRET", "https://host/#SECRET", "https://user:SECRET@host", "https://clean:9000/path"} {
		dir := t.TempDir()
		j, err := journal.Create(dir, journal.Identity{Source: []journal.Field{{Name: "endpoint", Value: ep, Canonical: ep}}}, []string{"list", "s3://bkt", "--endpoint", ep})
		if err != nil {
			t.Fatal(err)
		}
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		before, err := journal.Read(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{dir}, {"--journal", dir}} {
			var out bytes.Buffer
			if err := show(args, &out); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(out.Bytes(), []byte("SECRET")) {
				t.Errorf("credential in show: %s", out.Bytes())
			}
			if ep == "https://clean:9000/path" && !bytes.Contains(out.Bytes(), []byte(ep)) {
				t.Errorf("clean endpoint missing in show: %s", out.Bytes())
			}
		}
		after, err := journal.Read(dir)
		if err != nil || before.Header.Identity.Hash() != after.Header.Identity.Hash() || after.Header.Identity.Source[0].Value != ep || after.Header.Argv[len(after.Header.Argv)-1] != ep {
			t.Fatalf("show changed stored identity or arguments: %v", err)
		}
	}
}

func TestRootUserinfoSecrecy(t *testing.T) {
	for i, root := range []string{"s3://user:SECRET@bkt", "gs://user:SECRET@bkt", "az://user:SECRET@acct/ctr", "unknown://user:SECRET@host", "unknown://host/?sig=SECRET", "unknown://host/#SECRET", "s3://bkt?sig=SECRET", "gs://bkt#SECRET", "az://acct?sig=SECRET/ctr"} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			stderr := captureStderr(t)
			report := filepath.Join(t.TempDir(), "report.json")
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			var stdout bytes.Buffer
			if code := exitCode(t, run(ctx, []string{root, "--anonymous", "--endpoint", "http://127.0.0.1:1", "--report", report}, &stdout, diagList)); code != exitUsage {
				t.Fatalf("exit %d, want usage", code)
			}
			raw, err := os.ReadFile(report)
			if err != nil {
				t.Fatal(err)
			}
			for _, data := range [][]byte{stderr.Bytes(), stdout.Bytes(), raw} {
				if bytes.Contains(data, []byte("SECRET")) {
					t.Errorf("credential in output: %s", data)
				}
			}
		})
	}
}
