// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package main

import (
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// A group is a started command. Process groups, a parent-death signal and
// an exit observed without reaping are Linux facilities here, so
// elsewhere stopping reaches the launcher only, a launcher's descendants
// can outlive it, and a test binary killed outright leaves the launcher
// running. The launcher is reaped as soon as it exits; stop signals it by
// its process handle, which stays valid until then, and is serialized and
// does nothing once retired.
type group struct {
	cmd    *exec.Cmd
	exited chan struct{}

	mu      sync.Mutex
	retired bool
	waitErr error
}

func startGroup(cmd *exec.Cmd) (*group, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	g := &group{cmd: cmd, exited: make(chan struct{})}
	go func() {
		g.waitErr = cmd.Wait()
		if errors.Is(g.waitErr, exec.ErrWaitDelay) {
			g.waitErr = nil
		}
		close(g.exited)
	}()
	return g, nil
}

func (g *group) stop(grace time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.retired {
		return
	}
	exited := func() bool {
		select {
		case <-g.exited:
			return true
		default:
			return false
		}
	}
	if !exited() {
		g.cmd.Process.Signal(syscall.SIGTERM)
		if !waitFor(exited, grace) {
			g.cmd.Process.Kill()
			waitFor(exited, 10*time.Second)
		}
	}
	g.retired = exited()
}

func (g *group) clean() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.retired
}

func (g *group) kill() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.retired {
		g.cmd.Process.Kill()
	}
}
