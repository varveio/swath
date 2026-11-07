// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
)

// startJava starts the Java launcher's server for a test and stops it in
// a cleanup, which runs on every path out of the test (a failure, a
// panic, t.Fatal): the server's process group gets SIGTERM, then SIGKILL,
// and the cleanup checks the process is gone.
func startJava(t *testing.T, bin string, args []string, logPath string) *process {
	t.Helper()
	p, err := startServer(context.Background(), "java", bin, args, logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.stop()
		if err := p.cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("the Java server (pid %d) is still running after cleanup: %v", p.cmd.Process.Pid, err)
		}
	})
	return p
}
