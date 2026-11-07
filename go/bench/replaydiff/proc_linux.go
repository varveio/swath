// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// A group is a started command and the process group it leads. It owns
// the group's id for as long as it may signal it: the leader is not
// reaped until cleanup has finished (its exit is observed with
// waitid(WNOWAIT)), and while the leader's pid is allocated, even as a
// zombie, no other process can have that pid or lead a group with that
// id, so every group signal reaches this group or nobody. Once the leader
// is reaped the group is retired and never signalled again; cleanup is
// serialized, so a second stop, concurrent or later, does nothing.
//
// Other limits: the parent-death signal asked for at Start reaches the
// leader only, not its descendants, and fires when the OS thread that
// started it exits, which a Go program rarely does but may; a test binary
// killed outright runs no cleanup, so a descendant that ignores the
// leader's death can survive it.
type group struct {
	cmd    *exec.Cmd
	exited chan struct{} // closed when the leader has exited (not reaped)

	mu      sync.Mutex
	retired bool
	empty   bool  // the group had no member left when it was retired
	waitErr error // the leader's Wait
}

// killGroup signals a process group; tests wrap it to check every signal
// targets an id the group still owns.
var killGroup = func(pgid int, sig syscall.Signal) error { return syscall.Kill(-pgid, sig) }

// startGroup starts cmd as the leader of a process group of its own, with
// a parent-death SIGKILL.
func startGroup(cmd *exec.Cmd) (*group, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	g := &group{cmd: cmd, exited: make(chan struct{})}
	go func() {
		var info unix.Siginfo
		for unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil) == unix.EINTR {
		}
		close(g.exited)
	}()
	return g, nil
}

// members reports whether a process other than the leader, and not a
// zombie, is still in the group.
func (g *group) members() bool {
	pgid := g.cmd.Process.Pid
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == pgid {
			continue
		}
		stat, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		// Fields after the command's closing parenthesis: state, ppid, pgrp.
		i := bytes.LastIndexByte(stat, ')')
		if i < 0 {
			continue
		}
		f := bytes.Fields(stat[i+1:])
		if len(f) >= 3 && string(f[0]) != "Z" && string(f[2]) == strconv.Itoa(pgid) {
			return true
		}
	}
	return false
}

// stop stops the group: SIGTERM to it, grace for the leader to exit and
// every other member to go, then SIGKILL to it and up to ten seconds more,
// then the leader is reaped and the group retired. It is serialized and
// does nothing once the group is retired.
func (g *group) stop(grace time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.retired {
		return
	}
	pgid := g.cmd.Process.Pid
	stopped := func() bool {
		select {
		case <-g.exited:
			return !g.members()
		default:
			return false
		}
	}
	if !stopped() {
		killGroup(pgid, syscall.SIGTERM)
		if !waitFor(stopped, grace) {
			killGroup(pgid, syscall.SIGKILL)
			waitFor(stopped, 10*time.Second)
		}
	}
	g.empty = stopped()
	if !g.empty {
		// The leader is unkillable or a member is stuck: reaping would
		// release the id while the group may still exist, so keep it.
		return
	}
	g.waitErr = g.cmd.Wait()
	if errors.Is(g.waitErr, exec.ErrWaitDelay) {
		g.waitErr = nil
	}
	g.retired = true
}

// clean reports whether the group was retired with no member left.
func (g *group) clean() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.retired && g.empty
}

// kill sends SIGKILL to the whole group unless it is retired.
func (g *group) kill() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.retired {
		killGroup(g.cmd.Process.Pid, syscall.SIGKILL)
	}
}
