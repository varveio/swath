// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestTreeRunInterruptedNotResumable: a dataset run of the folder walk
// that stops (here at --max-duration, while a folder's page is held at the
// fake) is not resumable: the run said so when it started, its outcome
// says resumable false and offers no swath resume, and swath resume of the
// directory is refused.
func TestTreeRunInterruptedNotResumable(t *testing.T) {
	s3test.NoCredentials(t)
	f := s3test.NewR2([]string{"a/1", "a/2", "b/1", "c"})
	held := make(chan struct{})
	var once sync.Once
	f.Hook = func(r *http.Request) {
		if r.URL.Query().Get("prefix") == "b/" {
			once.Do(func() { close(held) })
			<-r.Context().Done()
		}
	}
	u := s3test.Serve(t, f)
	dir := filepath.Join(t.TempDir(), "ds")
	stderr := captureStderr(t)
	var out bytes.Buffer
	err := run(context.Background(), []string{"s3://" + s3test.Name, "--endpoint-url", u, "--anonymous", "--s3-region", "us-east-1",
		"--s3-listing", "tree", "-o", dir, "--max-duration", "2s"}, &out, false)
	select {
	case <-held:
	default:
		t.Fatalf("the barrier did not engage: exit %d: %v\n%s", exitOf(err), err, stderr)
	}
	if exitOf(err) != exitDeadline || !strings.Contains(stderr.String(), "swath resume cannot continue it") || strings.Contains(stderr.String(), "next: swath resume") {
		t.Fatalf("exit %d: %v\n%s", exitOf(err), err, stderr)
	}
	b, err := os.ReadFile(filepath.Join(dir, errorName))
	if err != nil {
		t.Fatal(err)
	}
	var o outcome
	if err := json.Unmarshal(b, &o); err != nil || o.Resumable {
		t.Fatalf("outcome %s (%v)", b, err)
	}
	err = resume(context.Background(), []string{dir}, &out)
	if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "nothing to resume") {
		t.Fatalf("resume: exit %d: %v", exitOf(err), err)
	}
}
