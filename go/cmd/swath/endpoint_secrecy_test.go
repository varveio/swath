// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"html"
	"io"
	"io/fs"
	"net/http"
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
						args = append(args, "--endpoint-url="+endpoint)
					} else {
						args = append(args, "--endpoint-url", endpoint)
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
		j, err := journal.Create(dir, journal.Identity{Source: []journal.Field{{Name: "endpoint", Value: ep, Canonical: ep}}}, []string{"list", "s3://bkt", "--endpoint-url", ep})
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
			if code := exitCode(t, run(ctx, []string{root, "--anonymous", "--endpoint-url", "http://127.0.0.1:1", "--report", report}, &stdout, diagList)); code != exitUsage {
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

// TestSecrecyRunOutputs extends the endpoint and root secrecy above to the
// outputs a run adds with -vvv, --log-format json and --report -: the
// request and retry events, the persistent-failure warning, the outcome
// event and the report on stdout. A refused endpoint or root holds its
// credential back from all of them; so does an Azure SAS, in the root or
// --azure-sas-token, sent to an endpoint whose every answer is a 503
// quoting the request.
func TestSecrecyRunOutputs(t *testing.T) {
	persistFor = 0 // the first failed request warns: no race with the stall timeout
	t.Cleanup(func() { persistFor = 10 * time.Second })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-error-code", "ServerBusy")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>ServerBusy</Code><Message>busy: %s</Message></Error>`, html.EscapeString(r.URL.String()))
	}))
	t.Cleanup(srv.Close)
	const sas = "sv=2026-10-06&sig=SECRET"
	for _, c := range []struct {
		name     string
		args     []string
		requests bool // the run reaches the endpoint
	}{
		{"endpoint-query", []string{"s3://bkt", "--anonymous", "--endpoint-url", "https://127.0.0.1:1/?sig=SECRET"}, false},
		{"endpoint-userinfo", []string{"gs://bkt", "--anonymous", "--endpoint-url=https://user:SECRET@127.0.0.1:1"}, false},
		{"root-userinfo", []string{"s3://user:SECRET@bkt", "--anonymous"}, false},
		{"root-sas", []string{"az://acct/ctr?" + sas, "--endpoint-url", srv.URL}, true},
		{"flag-sas", []string{"az://acct/ctr", "--endpoint-url", srv.URL, "--azure-sas-token", sas}, true},
	} {
		for _, format := range []string{logText, logJSON} {
			t.Run(c.name+"/"+format, func(t *testing.T) {
				stderr := captureSyncStderr(t)
				cmd := []string{"list"}
				if len(unjournaled) > 0 {
					cmd = []string{"diag", "list"} // --no-journal is a diagnostic flag
				}
				argv := append(append([]string{"--log-format", format, "-vvv"}, cmd...), c.args...)
				argv = append(append(argv, "--stall-timeout", "1s", "--report", "-", "-o", filepath.Join(t.TempDir(), "ds")), unjournaled...)
				var stdout bytes.Buffer
				if code := conclude(dispatch(t.Context(), argv, &stdout)); code == exitCompleted {
					t.Fatalf("exit 0; stderr %q", stderr.String())
				}
				for what, b := range map[string][]byte{"stderr": []byte(stderr.String()), "stdout": stdout.Bytes()} {
					if bytes.Contains(b, []byte("SECRET")) {
						t.Errorf("%s holds the credential: %s", what, b)
					}
				}
				if !bytes.Contains(stdout.Bytes(), []byte(`"report_version"`)) {
					t.Errorf("no report on stdout: %s", stdout.Bytes())
				}
				if !c.requests {
					return
				}
				for _, ev := range []string{evRequest, evRetry, evWarning, evOutcome} {
					want := `"event":"` + ev + `"`
					if format == logText {
						want = map[string]string{evRequest: "swath: request GET ", evRetry: "swath: attempt 1 ", evWarning: "swath: warning: Azure Blob ", evOutcome: "INCOMPLETE (stuck)"}[ev]
					}
					if !strings.Contains(stderr.String(), want) {
						t.Errorf("no %s event (%q) on stderr: %s", ev, want, stderr.String())
					}
				}
			})
		}
	}
}
