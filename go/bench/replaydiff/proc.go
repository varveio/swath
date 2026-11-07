// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// process is one replay server process under comparison.
type process struct {
	name     string
	cmd      *exec.Cmd
	endpoint string // http://host:port
	metrics  string // http://host:port, empty when the metrics port is off
	exited   chan struct{}
}

var (
	endpointRe = regexp.MustCompile(`(?:^| )endpoint=(http://\S+)`)
	metricsRe  = regexp.MustCompile(`metrics_endpoint=(http://\S+)/metrics`)
)

// startServer runs `bin serve args...`, waits for its startup line and its
// health endpoint, and copies its stderr to logPath. ctx bounds the wait
// for readiness (ten minutes at most). Once the command has started the
// process is returned with any error, already stopped, so a caller can
// confirm it is gone.
func startServer(ctx context.Context, name, bin string, args []string, logPath string) (*process, error) {
	cmd := exec.Command(bin, append([]string{"serve"}, args...)...)
	cmd.Env = os.Environ()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = io.Discard
	// A descendant that outlives the server and keeps its stderr open
	// must not keep Wait (and so stop) waiting forever.
	cmd.WaitDelay = 5 * time.Second
	detach(cmd)
	logf, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		logf.Close()
		return nil, err
	}
	s := &process{name: name, cmd: cmd, exited: make(chan struct{})}
	startup := make(chan string, 1)
	go func() {
		defer logf.Close()
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		sent := false
		for sc.Scan() {
			line := sc.Text()
			fmt.Fprintln(logf, line)
			if !sent && strings.HasPrefix(line, "swath_replay endpoint=") {
				startup <- line
				sent = true
			}
		}
	}()
	go func() { cmd.Wait(); close(s.exited) }()
	select {
	case line := <-startup:
		s.endpoint = endpointRe.FindStringSubmatch(line)[1]
		if m := metricsRe.FindStringSubmatch(line); m != nil {
			s.metrics = m[1]
		}
	case <-s.exited:
		return s, fmt.Errorf("%s exited before its startup line; see %s", name, logPath)
	case <-time.After(10 * time.Minute):
		s.stop()
		return s, fmt.Errorf("%s printed no startup line in 10 minutes; see %s", name, logPath)
	case <-ctx.Done():
		s.stop()
		return s, fmt.Errorf("%s printed no startup line before its deadline: %w; see %s", name, ctx.Err(), logPath)
	}
	if s.metrics != "" {
		deadline := time.Now().Add(10 * time.Minute)
		health := &http.Client{Timeout: 5 * time.Second}
		for {
			if ctx.Err() != nil {
				s.stop()
				return s, fmt.Errorf("%s: health endpoint not ready before its deadline: %w", name, ctx.Err())
			}
			resp, err := health.Get(s.metrics + "/healthz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					break
				}
			}
			if time.Now().After(deadline) {
				s.stop()
				return s, errors.New(name + ": health endpoint never answered")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return s, nil
}

// stop sends SIGTERM to the server's process group, kills the group if
// the server has not exited ten seconds later, and waits for it. It is
// safe to call more than once.
func (s *process) stop() {
	if s == nil || s.cmd.Process == nil {
		return
	}
	select {
	case <-s.exited:
		return
	default:
	}
	if signalGroup(s.cmd.Process, syscall.SIGTERM) != nil {
		s.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
		if signalGroup(s.cmd.Process, syscall.SIGKILL) != nil {
			s.cmd.Process.Kill()
		}
		<-s.exited
	}
}

// waitFor polls done until it holds or within passes.
func waitFor(done func() bool, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for !done() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// runBounded runs a one-shot command (the Java sort-fixture) as the
// servers are run: in a process group of its own, the whole group killed
// when ctx ends, Wait bounded when a descendant keeps the output pipe
// open, and any member left after the command returns killed and waited
// for, so neither a stuck command nor its descendants outlive the caller.
func runBounded(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	detach(cmd)
	cmd.Cancel = func() error { return signalGroup(cmd.Process, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	if cmd.Process != nil {
		signalGroup(cmd.Process, syscall.SIGKILL)
		waitFor(func() bool { return !groupAlive(cmd.Process) }, 10*time.Second)
	}
	return out, err
}
