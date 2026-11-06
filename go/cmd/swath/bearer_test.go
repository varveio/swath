// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// TestBearerToken: --bearer-token-command authenticates every request
// with "Authorization: Bearer" instead of SigV4; the command is never
// recorded (journal, report), a resume without it is refused with a clear
// message (exit 2) and continues with it; a failing command fails the run
// closed, class auth.
func TestBearerToken(t *testing.T) {
	b := s3mem.New()
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for _, k := range gcstest.Keys(3000) {
		b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil)
	}
	fake := gofakes3.New(b).Server()
	var mu sync.Mutex
	auths := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auths[r.Header.Get("Authorization")]++
		mu.Unlock()
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	secret := "printf tok-SECRET"
	dir := filepath.Join(t.TempDir(), "ds")
	args := []string{"s3://bkt", "--endpoint-url", srv.URL, "--bearer-token-command", secret, "-o", dir, "--part-size", "4KiB", "--part-interval", "0", "--workers", "2"}
	crashRun(t, args, 2)
	mu.Lock()
	for a := range auths {
		if a != "Bearer tok-SECRET" {
			t.Fatalf("authorization %q", a)
		}
	}
	mu.Unlock()
	st, err := journal.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(st.Header.Argv, " "), "SECRET") || !slices.Contains(st.Header.Argv, "--bearer-token-command=[redacted]") {
		t.Fatalf("recorded argv %q", st.Header.Argv)
	}
	err = dispatch(context.Background(), []string{"resume", dir}, io.Discard)
	if exitCode(t, err) != exitUsage || !strings.Contains(err.Error(), "--bearer-token-command, which is never recorded") {
		t.Fatalf("resume without the command: %v", err)
	}
	report := filepath.Join(t.TempDir(), "r.json")
	if err := dispatch(context.Background(), []string{"resume", dir, "--bearer-token-command", secret, "--report", report}, io.Discard); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(report)
	if bytes.Contains(raw, []byte("SECRET")) || !bytes.Contains(raw, []byte("[redacted]")) {
		t.Fatalf("report: %s", raw)
	}
	// A failing command: no request goes out, the run fails closed.
	bad := []string{"s3://bkt", "--endpoint", srv.URL, "--bearer-token-command", "echo nope >&2; exit 4", "--count-only", "--report", report}
	err = run(context.Background(), bad, io.Discard, false)
	if exitCode(t, err) != exitFatal || !strings.Contains(err.Error(), "exited 4: nope") {
		t.Fatalf("failing command: %v", err)
	}
	if raw, _ := os.ReadFile(report); !bytes.Contains(raw, []byte(`"cause": "auth"`)) {
		t.Fatalf("failing command report: %s", raw)
	}
	if err := run(context.Background(), []string{"s3://bkt", "--endpoint", srv.URL, "--bearer-token-command", "x", "--anonymous", "--count-only"}, io.Discard, false); exitCode(t, err) != exitUsage {
		t.Fatalf("--anonymous with a token: %v", err)
	}
}

func TestRedactArgv(t *testing.T) {
	got := redactArgv([]string{"list", "--bearer-token-command", "a b", "-bearer-token-command=c", "--x", "--bearer-token-refresh-interval", "1m"})
	want := []string{"list", "--bearer-token-command=[redacted]", "-bearer-token-command=[redacted]", "--x", "--bearer-token-refresh-interval", "1m"}
	if !slices.Equal(got, want) {
		t.Fatalf("%q", got)
	}
}
