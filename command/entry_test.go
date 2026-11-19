// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// TestHostProgram: a separate program that calls Main (testdata/host)
// behaves as the swath binary does: the same help, the same rows of a
// local listing, and the same refusal of a usage error with its exit code.
// Its --version is the binary's line followed by the host it is built into.
func TestHostProgram(t *testing.T) {
	swath := buildSwath(t)
	host := filepath.Join(t.TempDir(), "host")
	if runtime.GOOS == "windows" {
		host += ".exe"
	}
	if out, err := exec.Command("go", "build", "-tags", "swathtest", "-o", host, "./testdata/host").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	src := t.TempDir()
	if err := os.Mkdir(filepath.Join(src, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"a": "hi\n", "d/b": "x\n"} {
		if err := os.WriteFile(filepath.Join(src, filepath.FromSlash(name)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		stdout, stderr string
		code           int
	}
	call := func(bin string, args ...string) result {
		cmd := exec.Command(bin, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		code, _ := exitStatus(t, cmd.Run())
		return result{stdout.String(), stderr.String(), code}
	}
	for _, args := range [][]string{
		{"--help"},
		{"list", "--help"},
		{"list", src, "--format", "tsv", "--columns", "key,size,kind"},
		{"list"}, // no SOURCE: a usage error
	} {
		if got, want := call(host, args...), call(swath, args...); got != want {
			t.Errorf("host %q: %+v\nwant the binary's %+v", args, got, want)
		}
	}
	if got := call(host, "list"); got.code != exitUsage {
		t.Errorf("host list: exit %d, want %d", got.code, exitUsage)
	}
	if got := call(host, "list", src, "--format", "tsv", "--columns", "key,size,kind"); got.code != exitOK ||
		!strings.HasPrefix(got.stdout, "key\tsize\tkind\na\t3\tfile\n") || !strings.HasSuffix(got.stdout, "\nd/b\t2\tfile\n") {
		t.Errorf("host list: %+v", got)
	}

	got, want := call(host, "--version"), call(swath, "--version")
	if got.code != exitOK || want.code != exitOK || got.stdout != strings.TrimSuffix(want.stdout, "\n")+", the copy built into testhost\n" {
		t.Errorf("host --version: %+v\nthe binary's: %+v", got, want)
	}
}

// TestImportSetsNoPolicy: a fresh process that imports the package and
// lists without calling Main keeps the process-wide state Main sets as it
// was. The Azure SDK writes the log AZURE_SDK_GO_LOGGING asks for, which
// Main turns off (TestAzureSDKLogSuppressed, whose child this is in
// another mode), and SIGPIPE is not ignored (the child checks it).
func TestImportSetsNoPolicy(t *testing.T) {
	_, stderr := azlogChild(t, "import")
	if !slices.ContainsFunc(strings.Split(string(stderr), "\n"), sdkLogLine) {
		t.Fatalf("the SDK wrote no log line without Main:\n%s", stderr)
	}
}
