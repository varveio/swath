// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestAzureFlagsAndStartupLine serves the Azure route beside S3 and pins
// the startup line's protocol fields in the replay wire contract's
// spelling.
func TestAzureFlagsAndStartupLine(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals")
	}
	f, err := parseServe([]string{"--fixture", "f", "--bucket", "bkt", "--protocols", "azure, S3"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := f.config()
	if err != nil || cfg.AzureAccount != "replay" || strings.Join(cfg.Protocols, ",") != "azure,s3" {
		t.Fatalf("%+v %v", cfg, err)
	}
	fixture := filepath.Join(t.TempDir(), "keys.tsv")
	if err := os.WriteFile(fixture, []byte("a/1\t5\t1767225600000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "serve", "--fixture", fixture, "--bucket", "bkt", "--port", "0", "--protocols", "s3,azure",
		"--azure-account", "acct", "--advertised-host", "replay.example")
	cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1")
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	lines := bufio.NewScanner(stderr)
	var startup string
	for lines.Scan() {
		if strings.HasPrefix(lines.Text(), "swath_replay endpoint=") {
			startup = lines.Text()
			break
		}
	}
	port := regexp.MustCompile(`endpoint=http://127.0.0.1:(\d+) `).FindStringSubmatch(startup)
	if port == nil {
		t.Fatalf("startup: %s", startup)
	}
	for _, want := range []string{" protocols=azure,s3 ", " ordering_profile=unsigned-utf8-byte-order;azure=utf16-code-unit-order ",
		" profiles={azure=blob-list-flat-2026-06-06,2026-10-06, s3=list-objects-v2} ",
		" native_endpoints=s3:http://replay.example:" + port[1] + "/bkt?list-type=2,azure:http://replay.example:" + port[1] +
			"/acct/bkt?restype=container&comp=list"} {
		if !strings.Contains(startup+" ", want) {
			t.Errorf("startup lacks %q: %s", want, startup)
		}
	}
	req, _ := http.NewRequest("GET", "http://127.0.0.1:"+port[1]+"/acct/bkt?restype=container&comp=list", nil)
	req.Header.Set("x-ms-version", "2026-06-06")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), `ServiceEndpoint="http://replay.example:`+port[1]+`/acct/"`) ||
		!strings.Contains(string(b), "<Name>a/1</Name>") {
		t.Errorf("%d %s", resp.StatusCode, b)
	}
}
