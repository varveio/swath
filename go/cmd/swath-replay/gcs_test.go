// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

func TestProtocolSelection(t *testing.T) {
	base := []string{"--fixture", "f", "--bucket", "my-bucket"}
	for _, c := range []struct {
		protocols, want string
		ok              bool
	}{
		{"gcs", "gcs", true}, {"s3,gcs", "gcs,s3", true}, {" GCS , s3 ", "gcs,s3", true}, {"s3", "s3", true},
		{"azure", "", false}, {"s3,", "", false}, {"", "", false}, {"gcs,ftp", "", false},
	} {
		f, err := parseServe(append(append([]string{}, base...), "--protocols", c.protocols), io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := f.config()
		if (err == nil) != c.ok || (c.ok && cfg.ProtocolList() != c.want) {
			t.Errorf("%q: %q %v", c.protocols, cfg.ProtocolList(), err)
		}
	}
	for _, bucket := range []string{"storage", "Bad_Bucket", "x"} {
		f, _ := parseServe([]string{"--fixture", "f", "--bucket", bucket, "--protocols", "s3,gcs"}, io.Discard)
		if _, err := f.config(); err == nil {
			t.Errorf("bucket %q accepted with gcs", bucket)
		}
	}
}

// TestServeGCSEndToEnd runs the command with both routes: the startup
// line names them and their endpoints, and each answers.
func TestServeGCSEndToEnd(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals")
	}
	fixture := filepath.Join(t.TempDir(), "keys.tsv")
	if err := os.WriteFile(fixture, []byte("a/1\t5\t1767225600000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "serve", "--fixture", fixture, "--bucket", "bkt", "--port", "0", "--protocols", "gcs,s3")
	cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1")
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	lines := bufio.NewScanner(stderr)
	var startup string
	for lines.Scan() {
		if strings.HasPrefix(lines.Text(), "swath_replay endpoint=") {
			startup = lines.Text()
			break
		}
	}
	endpoint := regexp.MustCompile(`endpoint=(http://[^ ]+)`).FindStringSubmatch(startup)[1]
	if !strings.Contains(startup, " protocols=gcs,s3 ") || !strings.Contains(startup, " profiles={gcs=json-v1, s3=list-objects-v2} ") ||
		!strings.HasSuffix(startup, " native_endpoints=s3:"+endpoint+"/bkt?list-type=2,gcs:"+endpoint+"/storage/v1/b/bkt/o") {
		t.Errorf("startup: %s", startup)
	}
	if body := fetch(t, endpoint+"/storage/v1/b/bkt/o"); !strings.Contains(body, `"name":"a/1","size":"5","updated":"2026-01-01T00:00:00.000000Z"`) {
		t.Errorf("gcs: %s", body)
	}
	if body := fetch(t, endpoint+"/bkt?list-type=2"); !strings.Contains(body, "<Key>a/1</Key>") {
		t.Errorf("s3: %s", body)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	io.Copy(io.Discard, stderr)
	cmd.Wait()
}
