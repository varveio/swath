// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
)

// javaReady bounds how long a test waits for the Java server to be
// ready; javaRequests bounds each request a test sends either server.
const (
	javaReady    = 2 * time.Minute
	javaRequests = 30 * time.Second
)

// launch starts a server within ready and returns it with stopAndCheck,
// which stops it (process group SIGTERM, then SIGKILL) and reports an
// error if the process is still there. The process is returned whenever
// it started, also with an error.
func launch(bin string, args []string, logPath string, ready time.Duration) (*process, func() error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ready)
	defer cancel()
	p, err := startServer(ctx, "java", bin, args, logPath)
	stopAndCheck := func() error {
		if p == nil {
			return nil
		}
		p.stop()
		if err := p.cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("the server (pid %d) is still running after it was stopped: %v", p.cmd.Process.Pid, err)
		}
		return nil
	}
	return p, stopAndCheck, err
}

// startJava starts the Java launcher's server for a test and stops it in
// a cleanup registered as soon as the process exists, which runs on every
// path out of the test (a failure, a panic, t.Fatal), including a server
// that never became ready; the cleanup checks the process is gone.
func startJava(t *testing.T, bin string, args []string, logPath string) *process {
	t.Helper()
	p, stopAndCheck, err := launch(bin, args, logPath, javaReady)
	t.Cleanup(func() {
		if err := stopAndCheck(); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
