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
	for _, c := range []struct{ flag, root, endpoint string }{
		{"endpoint", "s3://bkt", s3.URL},
		{"gcs-endpoint", "gs://bkt", gcstest.Bucket(t, []string{"a", "b"}, 0)},
		{"azblob-endpoint", "az://replay/corpus", azEndpoint(az)},
	} {
		for _, eq := range []bool{false, true} {
			for _, success := range []bool{false, true} {
				t.Run(c.flag+"/equals="+strconv.FormatBool(eq)+"/success="+strconv.FormatBool(success), func(t *testing.T) {
					u, err := url.Parse(c.endpoint)
					if err != nil {
						t.Fatal(err)
					}
					u.User = url.UserPassword("PRIVATE_USER", "PRIVATE_PASSWORD")
					flagArgs := func(ep string) []string {
						if eq {
							return []string{"--" + c.flag + "=" + ep}
						}
						return []string{"--" + c.flag, ep}
					}
					dir := filepath.Join(t.TempDir(), "ds")
					report := filepath.Join(t.TempDir(), "report.json")
					base := []string{c.root, "--anonymous", "-o", dir, "--report", report}
					args := append(slices.Clone(base), flagArgs(u.String())...)
					var out bytes.Buffer
					if success {
						// Current options are clean; the recorded command may predate
						// the refusal. Listing must sanitize its own copy as well.
						err = runWith(context.Background(), command{}, append(slices.Clone(base), flagArgs(c.endpoint)...), &out, append([]string{"list"}, args...), false, nil)
					} else {
						err = run(context.Background(), args, io.Discard, false)
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
