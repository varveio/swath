// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package tokensource

import (
	"context"
	"os/exec"
)

// shell is the command interpreter the token command runs under.
const shell = "/bin/sh -c"

// shellCommand runs command with /bin/sh -c.
func shellCommand(ctx context.Context, command string) *exec.Cmd {
	return exec.CommandContext(ctx, "/bin/sh", "-c", command)
}
