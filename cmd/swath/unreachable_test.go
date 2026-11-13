// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// closedPort is a local address nothing listens on: connections to it
// are refused.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := ln.Addr().String()
	ln.Close()
	return host
}

// shortGrace shortens the clients' fail-fast grace for one test.
func shortGrace(t *testing.T, d time.Duration) {
	g := objhttp.UnreachableGrace
	objhttp.UnreachableGrace = d
	t.Cleanup(func() { objhttp.UnreachableGrace = g })
}

// unreachableReport is the part of the report these tests read.
type unreachableReport struct {
	Outcome listing.Outcome `json:"outcome"`
	Source  struct {
		Stats struct {
			Counters struct {
				Values map[string]any `json:"values"`
			} `json:"counters"`
		} `json:"stats"`
	} `json:"source"`
}

func (r *unreachableReport) value(name string) int64 {
	f, _ := r.Source.Stats.Counters.Values[name].(float64)
	return int64(f)
}

// TestUnreachableEndpoint: an endpoint override that refuses every
// connection, nothing ever answering there, ends the run fatal (exit 1,
// cause unreachable) once the grace has passed, long before the stall
// timeout, for S3, GCS and Azure Blob alike: the message names the
// endpoint and the cause, and the report counts the fail-fast.
func TestUnreachableEndpoint(t *testing.T) {
	stderr := captureSyncStderr(t)
	s3test.NoCredentials(t)
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	shortGrace(t, time.Second)
	host := closedPort(t)
	ep := "http://" + host
	for _, c := range []struct {
		name string
		args []string
	}{
		{"s3", []string{"s3://bkt/p/", "--endpoint-url", ep, "--s3-path-style", "--s3-region", "us-east-1"}},
		{"gcs", []string{"gs://bkt/p/", "--endpoint-url", ep + gcstest.Path}},
		{"azure", []string{"az://acct/c/p/", "--endpoint-url", ep, "--azure-assume-flat"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			report := filepath.Join(t.TempDir(), "report.json")
			args := append(c.args, "--anonymous", "--workers", "2", "--stall-timeout", "2m", "--report", report)
			t0 := time.Now()
			err := run(context.Background(), args, io.Discard, false)
			took := time.Since(t0)
			if code := exitCode(t, err); code != exitFatal {
				t.Fatalf("exit %d after %s (%v)", code, took, err)
			}
			// Generous: Windows takes about 2 s to refuse a local connection.
			if took > 30*time.Second {
				t.Fatalf("failed after %s, not within the grace", took)
			}
			if !strings.Contains(err.Error(), "endpoint "+ep+" cannot be reached") || !strings.Contains(err.Error(), "check the endpoint address") {
				t.Fatalf("the error does not name the endpoint: %v", err)
			}
			var rep unreachableReport
			readJSON(t, report, &rep)
			o := rep.Outcome
			if o.Class != listing.ClassFatal || o.ExitCode != exitFatal || o.Cause != string(objhttp.ClassUnreachable) || o.Resumable {
				t.Fatalf("outcome %+v", o)
			}
			if rep.value("unreachable.stops") < 1 || rep.value("unreachable.attempts") < 2 {
				t.Fatalf("the fail-fast is not in the report: %v", rep.Source.Stats.Counters.Values)
			}
		})
	}
	// The persistent-failure warning, when it came first, said how long.
	if strings.Contains(stderr.String(), "(retrying;") && !strings.Contains(stderr.String(), "nothing has answered: the run stops 1s after the first attempt)") {
		t.Fatalf("the warning does not say how long retrying lasts: %s", stderr.String())
	}
}

// TestUnreachableGraceRecovers: an endpoint that starts answering inside
// the grace (an emulator still starting) is listed as if it had been up.
func TestUnreachableGraceRecovers(t *testing.T) {
	captureSyncStderr(t)
	s3test.NoCredentials(t)
	shortGrace(t, 20*time.Second)
	host := closedPort(t)
	srv := &http.Server{Handler: s3test.Handler(s3test.Backend(t, s3test.Keys(5, 1)))}
	t.Cleanup(func() { srv.Close() })
	started := make(chan error, 1)
	time.AfterFunc(time.Second, func() {
		ln, err := net.Listen("tcp", host)
		started <- err
		if err == nil {
			go srv.Serve(ln)
		}
	})
	var out strings.Builder
	err := run(context.Background(), []string{"s3://bkt/", "--endpoint-url", "http://" + host, "--s3-path-style", "--s3-region", "us-east-1", "--anonymous",
		"--columns", "key", "--format", "tsv"}, &out, false)
	if lerr := <-started; lerr != nil {
		t.Skipf("the port was taken meanwhile: %v", lerr)
	}
	if err != nil {
		t.Fatalf("a server that started inside the grace: %v", err)
	}
	if n := strings.Count(out.String(), "\n"); n != 6 { // the header and five keys
		t.Fatalf("%d lines: %q", n, out.String())
	}
}

// TestUnreachableAfterAnswer: an endpoint that answered once and then
// refuses is an outage, not a wrong address: the run keeps retrying until
// the stall timeout stops it as stuck (75), whatever the grace.
func TestUnreachableAfterAnswer(t *testing.T) {
	captureSyncStderr(t)
	s3test.NoCredentials(t)
	shortGrace(t, 100*time.Millisecond)
	h := s3test.Handler(s3test.Backend(t, s3test.Keys(5, 1)))
	var n atomic.Int64
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 1 {
			panic(http.ErrAbortHandler) // a connection still open: cut
		}
		w.Header().Set("Connection", "close")
		h.ServeHTTP(w, r)
		srv.Listener.Close() // every later connection is refused
	}))
	t.Cleanup(srv.Close)
	report := filepath.Join(t.TempDir(), "report.json")
	err := run(context.Background(), []string{"s3://bkt/", "--endpoint-url", srv.URL, "--s3-path-style", "--s3-region", "us-east-1", "--anonymous",
		"--sequential", "--page-size", "1", "--stall-timeout", "2s", "--report", report}, io.Discard, true)
	if code := exitCode(t, err); code != exitStuck {
		t.Fatalf("exit %d (%v)", code, err)
	}
	var rep unreachableReport
	readJSON(t, report, &rep)
	if rep.value("unreachable.stops") != 0 || rep.Outcome.Class != listing.ClassStuck {
		t.Fatalf("outcome %+v, counters %v", rep.Outcome, rep.Source.Stats.Counters.Values)
	}
}
