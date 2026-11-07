// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package tokensource

import (
	"context"
	"os"
	"os/exec"
	"syscall"
)

// shell is the command interpreter the token command runs under.
const shell = "cmd.exe /d /s /c"

// shellCommand runs command with cmd.exe (%ComSpec%) /d /s /c "command":
// /d skips AutoRun, /s takes the quoted command verbatim. cmd.exe does not
// parse the escaping Go applies to arguments, so the command line is set
// whole rather than built from Args.
func shellCommand(ctx context.Context, command string) *exec.Cmd {
	comspec := os.Getenv("ComSpec")
	if comspec == "" {
		comspec = `C:\Windows\System32\cmd.exe`
	}
	cmd := exec.CommandContext(ctx, comspec)
	cmd.SysProcAttr = &syscall.SysProcAttr{CmdLine: `"` + comspec + `" /d /s /c "` + command + `"`}
	return cmd
}
