// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package main

import (
	"os"
	"os/exec"
	"syscall"
)

// detach leaves the process as started: process groups and a parent-death
// signal are Linux facilities here, so elsewhere stopping a server reaches
// the launcher only, and a launcher's descendants can outlive it.
func detach(*exec.Cmd) {}

// signalGroup signals the server's process.
func signalGroup(p *os.Process, sig syscall.Signal) error {
	if sig == syscall.SIGKILL {
		return p.Kill()
	}
	return p.Signal(sig)
}

// groupAlive is false: without process groups only the launcher, whose
// exit stop waits for, is tracked.
func groupAlive(*os.Process) bool { return false }
