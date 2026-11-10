// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package tokensource

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestShell: on unix the command runs under /bin/sh -c (shell_windows.go:
// cmd.exe on Windows, TestWindowsShell).
func TestShell(t *testing.T) {
	cmd := shellCommand(context.Background(), "echo hi")
	if want := []string{"/bin/sh", "-c", "echo hi"}; strings.Join(cmd.Args, "|") != strings.Join(want, "|") {
		t.Fatalf("%q", cmd.Args)
	}
}

// TestCommand: Java's contract: the token is the command's trimmed stdout,
// fetched again after the refresh interval; a failing command fails
// closed with its exit code and bounded, escaped stderr, never a stale
// token; String hides the command.
func TestCommand(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "n")
	cmd := fmt.Sprintf(`echo x >> %s; printf ' tok-%%s\n' "$(wc -l < %s | tr -d ' ')"`, counter, counter)
	c := &Command{Cmd: cmd, Refresh: 45 * time.Minute}
	s := NewShared(c)
	get := func() string {
		tok, err := s.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return tok.AccessToken
	}
	if a, b := get(), get(); a != "tok-1" || b != "tok-1" {
		t.Fatalf("%q %q", a, b)
	}
	if tok, _ := c.Token(); tok.Expiry.Sub(time.Now()) < 44*time.Minute {
		t.Fatalf("expiry %v: not the refresh interval", tok.Expiry)
	}
	if strings.Contains(c.String(), "echo") || strings.Contains(fmt.Sprint(c), counter) {
		t.Fatal("the command shows")
	}
	_ = os.Remove(counter)
	for _, tc := range []struct{ cmd, want string }{
		{"echo 'denied\x1b[2J' >&2; exit 3", `--bearer-token-command exited 3: denied\x1b[2J`},
		{"true", "produced no output"},
		{"printf 'a\\nb\\n'", "more than one line"},
	} {
		_, err := (&Command{Cmd: tc.cmd}).Token()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.cmd, err)
		}
	}
	if _, err := (&Command{Cmd: "sleep 5", Timeout: 100 * time.Millisecond}).Token(); err == nil || !strings.Contains(err.Error(), "did not exit") {
		t.Errorf("timeout: %v", err)
	}
}
