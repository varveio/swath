// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package main

import (
	"os"
	"os/exec"
	"syscall"
)

// detach starts a server in a process group of its own, so stopping it
// reaches every process the launcher started, and asks the kernel to kill
// it if this process dies first (a test binary killed by its timeout runs
// no cleanup).
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

// signalGroup signals the server's process group.
func signalGroup(p *os.Process, sig syscall.Signal) error {
	return syscall.Kill(-p.Pid, sig)
}
