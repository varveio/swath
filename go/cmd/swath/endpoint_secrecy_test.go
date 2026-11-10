// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
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
							return []string{"--endpoint-url=" + ep}
						}
						return []string{"--endpoint-url", ep}
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

// TestSecretsNeverEchoed: a credential typed on the command line is never
// echoed by an error about that command line: a misspelled or one-dash
// secret flag, a credential-bearing -o, SOURCE or extra argument, an
// unknown scheme. Neither stderr (text or JSON events) nor the report
// holds it.
func TestSecretsNeverEchoed(t *testing.T) {
	const secret = "TESTSECRET"
	sas := "sv=2024-01-01&sig=" + secret
	dir := t.TempDir()
	for _, args := range [][]string{
		{"list", "az://acct/c", "-azure-sas-token=" + secret},
		{"list", "az://acct/c", "--azure-sas-tokn=" + secret},
		{"list", "az://acct/c", "--azure-sas-tokn", secret},
		{"list", "az://acct/c", "-Z" + secret},
		{"list", "az://acct/c", "-Z=" + secret},
		{"list", "az://acct/c", "-o", "https://acct.blob.core.windows.net/c?" + sas},
		{"list", "az://acct/c", "-o", "az://acct/c?" + sas},
		{"list", "az://acct/c", "az://acct/d?" + sas},
		{"list", "bad://b?" + sas, "-o", filepath.Join(dir, "ds")},
		{"diag", "count", "az://acct/c", "--columns", "key", "az://acct/d?" + sas},
		{"resume", dir, "az://acct/c?" + sas},
		{"completion", "bash", "az://acct/c?" + sas},
		{"az://acct/c?" + sas},
	} {
		for _, format := range []string{logText, logJSON} {
			t.Run(format+"/"+strings.Join(args, " "), func(t *testing.T) {
				stderr := captureStderr(t)
				t.Cleanup(func() { logs.Store(nil) })
				report := filepath.Join(t.TempDir(), "report.json")
				var stdout bytes.Buffer
				argv := append([]string{"--log-format", format, "-v"}, args...)
				if args[0] == "list" || args[0] == "resume" {
					argv = append(argv, "--report", report)
				}
				if code := conclude(dispatch(context.Background(), argv, &stdout)); code == exitCompleted {
					t.Fatalf("exit 0; stderr %q", stderr.String())
				}
				raw, _ := os.ReadFile(report)
				for what, b := range map[string][]byte{"stderr": stderr.Bytes(), "stdout": stdout.Bytes(), "report": raw} {
					if bytes.Contains(b, []byte(secret)) {
						t.Errorf("%s holds the secret: %s", what, b)
					}
				}
				if stderr.Len() == 0 {
					t.Error("no error on stderr")
				}
			})
		}
	}
}
