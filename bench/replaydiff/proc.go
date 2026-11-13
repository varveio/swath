// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// process is one replay server process under comparison.
type process struct {
	name     string
	cmd      *exec.Cmd
	group    *group
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
// process is returned with any error, already stopped (its whole group,
// on every path after Start, including a launcher that exits before its
// startup line and leaves a child behind), so a caller can confirm it is
// gone and need not stop it.
func startServer(ctx context.Context, name, bin string, args []string, logPath string) (_ *process, err error) {
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
	logf, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	g, err := startGroup(cmd)
	if err != nil {
		logf.Close()
		return nil, err
	}
	s := &process{name: name, cmd: cmd, group: g, exited: g.exited}
	defer func() {
		if err != nil {
			s.stop()
		}
	}()
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
	select {
	case line := <-startup:
		s.endpoint = endpointRe.FindStringSubmatch(line)[1]
		if m := metricsRe.FindStringSubmatch(line); m != nil {
			s.metrics = m[1]
		}
	case <-s.exited:
		return s, fmt.Errorf("%s exited before its startup line; see %s", name, logPath)
	case <-time.After(10 * time.Minute):
		return s, fmt.Errorf("%s printed no startup line in 10 minutes; see %s", name, logPath)
	case <-ctx.Done():
		return s, fmt.Errorf("%s printed no startup line before its deadline: %w; see %s", name, ctx.Err(), logPath)
	}
	if s.metrics != "" {
		deadline := time.Now().Add(10 * time.Minute)
		health := &http.Client{Timeout: 5 * time.Second}
		for {
			if ctx.Err() != nil {
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
				return s, errors.New(name + ": health endpoint never answered")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return s, nil
}

// stop stops the server and every process of its group, whether or not
// the launcher has already exited (see group.stop: SIGTERM, ten seconds'
// grace, SIGKILL, then the group is retired). It is serialized and safe to
// call any number of times: once the group is retired it is never
// signalled again.
func (s *process) stop() {
	if s == nil || s.group == nil {
		return
	}
	s.group.stop(10 * time.Second)
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
// servers are run: as the leader of a process group of its own, the whole
// group killed when ctx ends, and, once the leader has exited, any member
// left killed and the group retired before the output is returned (Wait
// bounded when a descendant held the output pipe). The group's id stays
// owned until then (see group), so the sweep cannot reach another group.
func runBounded(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	cmd.WaitDelay = 5 * time.Second
	g, err := startGroup(cmd)
	if err != nil {
		return nil, err
	}
	select {
	case <-g.exited:
	case <-ctx.Done():
		g.kill()
	}
	g.stop(0)
	if !g.clean() {
		return nil, fmt.Errorf("%s: its process group did not stop", name)
	}
	if ctx.Err() != nil {
		return out.Bytes(), ctx.Err()
	}
	return out.Bytes(), g.waitErr
}
