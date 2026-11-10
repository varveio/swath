// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
)

// subcommand runs this test binary as swath-replay with args and returns
// its stdout, stderr and exit code.
func subcommand(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	code := 0
	if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return stdout.String(), stderr.String(), code
}

// writeCapture writes an unsorted Java-era capture of two parts.
func writeCapture(t *testing.T, parts ...[]legacytest.Row) string {
	t.Helper()
	dir := t.TempDir()
	for i, rows := range parts {
		name := filepath.Join(dir, "part-0000"+string(rune('0'+i))+".parquet")
		if err := legacytest.Write(name, legacytest.Part{Rows: rows}); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestSortFixtureThenServe sorts a capture with the command and serves its
// output through the S3 route: the listing comes back in unsigned byte
// order with the capture's facts, under the summary line the command
// prints on stdout.
func TestSortFixtureThenServe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals")
	}
	obj := legacytest.Object
	capture := writeCapture(t,
		[]legacytest.Row{obj("logs/b c", 3), obj("logs/a", 1), obj("z", 4)},
		[]legacytest.Row{obj("logs/é", 5), obj("a/1", 2)})
	output := filepath.Join(t.TempDir(), "sorted")

	stdout, stderr, code := subcommand(t, "sort-fixture", "--capture", capture, "--output="+output, "--key-limit", "2048")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	summary := regexp.MustCompile(`^sort_fixture rows=5 objects=5 skipped=0 parts=1 bytes=[1-9][0-9]* schema=swath-replay/objects/v1 wall_ms=[0-9]+ output=\S+\n$`)
	if !summary.MatchString(stdout) {
		t.Errorf("stdout %q", stdout)
	}

	cmd := exec.Command(os.Args[0], "serve", "--fixture", output, "--bucket", "bkt", "--port", "0", "--metrics-port", "0",
		"--parquet-connections", "2")
	cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1")
	errPipe, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	lines := bufio.NewScanner(errPipe)
	var startup string
	for lines.Scan() {
		if strings.HasPrefix(lines.Text(), "swath_replay endpoint=") {
			startup = lines.Text()
			break
		}
	}
	if !strings.Contains(startup, "serving_mode=SORTED") {
		t.Fatalf("startup: %q", startup)
	}
	endpoint := regexp.MustCompile(`endpoint=(http://[^ ]+)`).FindStringSubmatch(startup)[1]
	body := fetch(t, endpoint+"/bkt?list-type=2&encoding-type=url")
	keys := regexp.MustCompile(`<Key>([^<]*)</Key>`).FindAllStringSubmatch(body, -1)
	var got []string
	for _, k := range keys {
		got = append(got, k[1])
	}
	if want := "a/1 logs/a logs/b+c logs/%C3%A9 z"; strings.Join(got, " ") != want {
		t.Errorf("keys %q, want %q\n%s", strings.Join(got, " "), want, body)
	}
	if !strings.Contains(body, "<Key>logs/a</Key><LastModified>2026-01-01T00:00:01.000Z</LastModified>") ||
		!strings.Contains(body, "<Size>1</Size>") {
		t.Errorf("facts lost in the sort:\n%s", body)
	}
	body = fetch(t, endpoint+"/bkt?list-type=2&delimiter=/&max-keys=1")
	if !strings.Contains(body, "<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>") ||
		!strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
		t.Errorf("delimited page:\n%s", body)
	}
}

// TestListIndexLineFollowsTheStartupLine: serving a SeekParquet dataset,
// the startup line stays the first key=value line on stderr and the
// list_index line, with the build's stats, comes next; --list-index=false
// reports the index off.
func TestListIndexLineFollowsTheStartupLine(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals")
	}
	obj := legacytest.Object
	capture := writeCapture(t, []legacytest.Row{obj("logs/a/1", 1), obj("logs/a/2", 2), obj("logs/b/1", 3), obj("z", 4)})
	output := filepath.Join(t.TempDir(), "sorted")
	if _, stderr, code := subcommand(t, "sort-fixture", "--capture", capture, "--output="+output); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	for _, tc := range []struct {
		flag string
		want *regexp.Regexp
	}{
		{"--list-index=true", regexp.MustCompile(`^swath_replay list_index state=built max_bytes=67108864 workers=[1-9][0-9]* ` +
			`groups_indexed=[0-9]+ groups_skipped=[0-9]+ groups_failed=0 parts_failed=0 pages=[1-9][0-9]* entries=[0-9]+ bytes=[0-9]+ ` +
			`budget_exhausted=false has_index=(true|false) seconds=[0-9]+\.[0-9]{3}$`)},
		{"--list-index=false", regexp.MustCompile(`^swath_replay list_index state=off max_bytes=0 workers=0 .* seconds=0\.000$`)},
	} {
		cmd := exec.Command(os.Args[0], "serve", "--fixture", output, "--bucket", "bkt", "--port", "0", tc.flag)
		cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1")
		errPipe, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lines := bufio.NewScanner(errPipe)
		var got []string
		for len(got) < 2 && lines.Scan() {
			if strings.Contains(lines.Text(), "=") {
				got = append(got, lines.Text())
			}
		}
		cmd.Process.Kill()
		cmd.Wait()
		if len(got) != 2 || !strings.HasPrefix(got[0], "swath_replay endpoint=") || !tc.want.MatchString(got[1]) {
			t.Errorf("%s: first key=value lines %q", tc.flag, got)
		}
	}
}

// TestSortFixtureRefusals: a refusal is one line on stderr carrying the
// sorter's message, exit 1, and nothing on stdout; a bad command line is
// exit 2.
func TestSortFixtureRefusals(t *testing.T) {
	obj := legacytest.Object
	dup := writeCapture(t, []legacytest.Row{obj("a", 0), obj("b", 1)}, []legacytest.Row{obj("b", 2)})
	stdout, stderr, code := subcommand(t, "sort-fixture", "--capture", dup, "--output", filepath.Join(t.TempDir(), "out"))
	if code != 1 || stdout != "" || stderr != "sort-fixture: duplicate key 'b'\n" {
		t.Errorf("duplicate: exit %d stdout %q stderr %q", code, stdout, stderr)
	}

	empty := t.TempDir()
	_, stderr, code = subcommand(t, "sort-fixture", "--capture", empty, "--output", filepath.Join(t.TempDir(), "out"))
	if code != 1 || !strings.HasPrefix(stderr, "sort-fixture: no *.parquet parts in ") || strings.Count(stderr, "\n") != 1 {
		t.Errorf("no parts: exit %d stderr %q", code, stderr)
	}

	for _, args := range [][]string{
		{"sort-fixture", "--capture", dup},
		{"sort-fixture", "--output", empty},
		{"sort-fixture", "--capture", dup, "--output", empty, "extra"},
		{"sort-fixture", "--key-limit", "x"},
	} {
		if _, stderr, code := subcommand(t, args...); code != 2 {
			t.Errorf("%q: exit %d, stderr %q", args, code, stderr)
		}
	}
	_, stderr, code = subcommand(t, "sort-fixture", "--capture", dup, "--output", filepath.Join(t.TempDir(), "out"), "--key-limit", "5000")
	if code != 1 || !strings.HasPrefix(stderr, "sort-fixture: key limit 5000 outside") {
		t.Errorf("key limit: exit %d stderr %q", code, stderr)
	}
}
