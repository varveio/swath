// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command host embeds swath as another program does, for TestHostProgram:
// it names itself the host and runs the command line it is given through
// command.Main.
package main

import (
	"context"
	"os"

	"github.com/varveio/swath/command"
)

func main() {
	command.Exit(command.Main(context.Background(), "testhost", os.Args[1:], os.Stdout, os.Stderr))
}
