// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package tokensource

import (
	"strings"
	"testing"
)

// TestWindowsShell: the command runs under cmd.exe, quotes and all.
func TestWindowsShell(t *testing.T) {
	tok, err := (&Command{Cmd: `echo tok-"1"`}).Token()
	if err != nil || tok.AccessToken != `tok-"1"` {
		t.Fatalf("%v %v", tok, err)
	}
	if _, err := (&Command{Cmd: "echo denied 1>&2 & exit /b 3"}).Token(); err == nil || !strings.Contains(err.Error(), "exited 3: denied") {
		t.Fatalf("failure: %v", err)
	}
	if !strings.HasPrefix(shell, "cmd.exe") {
		t.Fatal(shell)
	}
}
