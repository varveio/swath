// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package command is the swath command line as a function another Go
// program can call, so that the program can build swath into its own binary
// and run it as a child process it starts. The swath binary is
// Exit(Main(...)) and nothing else (cmd/swath).
//
// Main is for a process dedicated to the call. It sets the command's
// process-wide policy when it is called, never when the package is imported,
// and does not undo it when it returns: it turns off the Azure SDK's own log,
// stops the run at SIGINT, SIGTERM and SIGHUP (unless SIGHUP was ignored at
// start), ignores SIGPIPE, and arms the forced exit of a run that a signal or
// a command-line timeout stopped and that has not finished stopping after a
// grace period. docs/library.md has the details.
//
// List runs the work of swath list in the calling process from a typed
// ListOptions instead of arguments, through the same code as Main, and
// returns a typed result whose error matches stable reasons with
// errors.Is. It sets no signal handling and no forced exit: the run stops
// when its context ends.
//
// The package is experimental: its API may change in any release.
package command

import (
	"context"
	"io"

	"github.com/varveio/swath/internal/command"
)

// Main runs the swath command line args, without the program name, and
// returns its exit code. hostName names the program this copy of swath is
// built into, and swath --version then says so; the swath binary passes "".
// Rows, help, the version and --report - go to stdout; the log, the progress
// line, the statistics and the outcome go to stderr. A run stops when ctx is
// canceled.
func Main(ctx context.Context, hostName string, args []string, stdout, stderr io.Writer) int {
	return command.Main(ctx, hostName, args, stdout, stderr)
}

// Exit ends the process with code as the swath binary does. On Unix a run
// that a signal stopped (exit 130, 143 or 129) dies by that signal; any other
// code, and every code on Windows, is os.Exit's.
func Exit(code int) {
	command.Exit(code)
}
