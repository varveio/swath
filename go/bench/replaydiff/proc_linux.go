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
// no cleanup). Limits: the parent-death signal reaches the launcher only,
// not its descendants, and fires when the OS thread that started it
// exits, which a Go program rarely does but may; the group's id is the
// launcher's pid, which the system may reuse once the whole group is gone.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

// signalGroup signals the server's process group.
func signalGroup(p *os.Process, sig syscall.Signal) error {
	return syscall.Kill(-p.Pid, sig)
}

// groupAlive reports whether any process of the server's group remains
// (a member that has exited but is not yet reaped still counts).
func groupAlive(p *os.Process) bool {
	err := syscall.Kill(-p.Pid, 0)
	return err == nil || err == syscall.EPERM
}
