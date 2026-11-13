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
// reaped until cleanup has finished, and while the leader's pid is
// allocated, even as a zombie, no other process can have that pid or
// lead a group with that id, so every group signal reaches this group or
// nobody. The leader's exit is observed without reaping it
// (waitid(WNOWAIT); where that call is refused, by its /proc state), and
// only this code reaps it. Should the leader be reaped elsewhere (ECHILD),
// ownership is lost and the group is never signalled by its id again.
//
// Cleanup quiesces the group before trusting an empty scan: it always
// ends with SIGKILL to the whole group while the leader still reserves
// the id, so no member can fork a successor the scan misses, and it then
// requires two consecutive /proc scans without a live member, a process
// counting as live while any of its threads is (a zombie main thread
// after pthread_exit can leave others running). A scan that cannot read
// /proc is never taken as empty. Once verified, the leader is reaped and
// the group retired; cleanup is serialized and does nothing once retired.
//
// Other limits: the parent-death signal asked for at Start reaches the
// leader only, not its descendants, and fires when the OS thread that
// started it exits, which a Go program rarely does but may; a test binary
// killed outright runs no cleanup, so a descendant that ignores the
// leader's death can survive it.
type group struct {
	cmd    *exec.Cmd
	exited chan struct{} // closed when the leader has exited (not reaped)

	mu sync.Mutex
	// sigMu orders ownership against signals: the loss of ownership is
	// published, and every group signal checks ownership and is sent,
	// each under it, so no signal follows a published loss. It is held
	// only for that, never while waiting.
	sigMu   sync.Mutex
	lost    bool // the leader was reaped elsewhere: never signal its id
	retired bool
	empty   bool  // the group was verified empty when retired
	waitErr error // the leader's Wait
}

// killGroup signals a process group; tests wrap it to check every signal
// targets an id the group still owns.
var killGroup = func(pgid int, sig syscall.Signal) error { return syscall.Kill(-pgid, sig) }

// observeExit waits, without reaping, until the leader pid has exited.
// Tests replace it to exercise the observer's failures.
var observeExit = func(pid int) error {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if err != unix.EINTR {
			return err
		}
	}
}

// procRoot is where processes are inspected; tests point it elsewhere to
// exercise an inspection failure.
var procRoot = "/proc"

// startGroup starts cmd as the leader of a process group of its own, with
// a parent-death SIGKILL.
func startGroup(cmd *exec.Cmd) (*group, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	g := &group{cmd: cmd, exited: make(chan struct{})}
	go g.watch()
	return g, nil
}

// watch closes exited once the leader has exited. A refused waitid
// (ENOSYS under a seccomp policy, or any other failure but ECHILD) falls
// back to polling the leader's /proc state, which observes the exit
// without reaping either; ECHILD means another waiter reaped the leader.
func (g *group) watch() {
	pid := g.cmd.Process.Pid
	err := observeExit(pid)
	switch {
	case err == nil:
	case errors.Is(err, unix.ECHILD):
		g.sigMu.Lock()
		g.lost = true
		g.sigMu.Unlock()
	default:
		for live, ok := liveProcess(pid); !ok || live; live, ok = liveProcess(pid) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	close(g.exited)
}

// liveProcess reports whether pid has a thread that has not exited; ok is
// false when that could not be read (ESRCH, the pid gone, reads as not
// live).
func liveProcess(pid int) (live, ok bool) {
	dir := procRoot + "/" + strconv.Itoa(pid) + "/task"
	tasks, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, true
	}
	if err != nil {
		return false, false
	}
	for _, t := range tasks {
		stat, err := os.ReadFile(dir + "/" + t.Name() + "/stat")
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, false
		}
		if st, _, ok := statFields(stat); !ok {
			return false, false
		} else if st != "Z" && st != "X" {
			return true, true
		}
	}
	return false, true
}

// statFields returns the state and process group of a /proc stat line.
func statFields(stat []byte) (state, pgrp string, ok bool) {
	i := bytes.LastIndexByte(stat, ')')
	if i < 0 {
		return "", "", false
	}
	f := bytes.Fields(stat[i+1:])
	if len(f) < 3 {
		return "", "", false
	}
	return string(f[0]), string(f[2]), true
}

// members reports whether a process other than the leader, with a thread
// still running, is in the group; ok is false when /proc could not be
// read, which is never taken as an empty group.
func (g *group) members() (any, ok bool) {
	pgid := strconv.Itoa(g.cmd.Process.Pid)
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return false, false
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == g.cmd.Process.Pid {
			continue
		}
		stat, err := os.ReadFile(procRoot + "/" + e.Name() + "/stat")
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, false
		}
		_, pgrp, sok := statFields(stat)
		if !sok {
			return false, false
		}
		if pgrp != pgid {
			continue
		}
		live, lok := liveProcess(pid)
		if !lok {
			return false, false
		}
		if live {
			return true, true
		}
	}
	return false, true
}

// stop stops the group: SIGTERM and grace when it is not already quiet,
// then always a final SIGKILL to the whole group while the leader still
// reserves its id, then up to ten seconds for the leader's exit and two
// consecutive verified-empty scans; only then is the leader reaped and the
// group retired. A group that cannot be verified is kept. A group whose
// leader was reaped elsewhere is retired without a signal. Serialized;
// does nothing once retired.
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
	quiet := func() bool {
		if !exited() {
			return false
		}
		any, ok := g.members()
		return ok && !any
	}
	// Each wait also ends when ownership is lost, and each signal is sent
	// only while ownership holds.
	if !g.owned() {
		g.retired = true
		return
	}
	if !quiet() {
		if g.send(syscall.SIGTERM) {
			waitFor(func() bool { return !g.owned() || quiet() }, grace)
		}
	}
	if !g.send(syscall.SIGKILL) {
		g.retired = true
		return
	}
	emptyRuns := 0
	verified := waitFor(func() bool {
		if !g.owned() {
			return true
		}
		if quiet() {
			emptyRuns++
		} else {
			emptyRuns = 0
		}
		return emptyRuns >= 2
	}, 10*time.Second)
	if !g.owned() {
		g.retired = true
		return
	}
	if !verified {
		// A member that cannot be made to go, or a /proc that cannot be
		// read: reaping would release the id, so keep it.
		return
	}
	g.empty = true
	g.waitErr = g.cmd.Wait()
	if errors.Is(g.waitErr, exec.ErrWaitDelay) {
		g.waitErr = nil
	}
	g.retired = true
}

// owned reports whether the group still owns its id.
func (g *group) owned() bool {
	g.sigMu.Lock()
	defer g.sigMu.Unlock()
	return !g.lost
}

// send signals the group if it still owns its id, checking and sending
// under sigMu; false when ownership is lost.
func (g *group) send(sig syscall.Signal) bool {
	g.sigMu.Lock()
	defer g.sigMu.Unlock()
	if g.lost {
		return false
	}
	killGroup(g.cmd.Process.Pid, sig)
	return true
}

// clean reports whether the group was retired and verified empty.
func (g *group) clean() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.retired && g.empty
}

// kill sends SIGKILL to the whole group unless it is retired or its
// ownership was lost.
func (g *group) kill() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.retired {
		g.send(syscall.SIGKILL)
	}
}
