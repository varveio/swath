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
// health endpoint, and copies its stderr to logPath.
func startServer(ctx context.Context, name, bin string, args []string, logPath string) (*process, error) {
	cmd := exec.Command(bin, append([]string{"serve"}, args...)...)
	cmd.Env = os.Environ()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = io.Discard
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
		return nil, fmt.Errorf("%s exited before its startup line; see %s", name, logPath)
	case <-time.After(10 * time.Minute):
		s.stop()
		return nil, fmt.Errorf("%s printed no startup line in 10 minutes; see %s", name, logPath)
	case <-ctx.Done():
		s.stop()
		return nil, ctx.Err()
	}
	if s.metrics != "" {
		deadline := time.Now().Add(10 * time.Minute)
		for {
			resp, err := http.Get(s.metrics + "/healthz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					break
				}
			}
			if time.Now().After(deadline) {
				s.stop()
				return nil, errors.New(name + ": health endpoint never answered")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return s, nil
}

// stop sends SIGTERM to the process (by pid), and kills it if it has not
// exited ten seconds later.
func (s *process) stop() {
	if s == nil || s.cmd.Process == nil {
		return
	}
	s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
		s.cmd.Process.Kill()
		<-s.exited
	}
}
