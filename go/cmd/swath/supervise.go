// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
)

// Exit codes: the outcome a supervising process acts on without reading
// stderr (error.json carries the same, with the cause).
const (
	exitCompleted   = 0   // the dataset is committed (or a dry run finished)
	exitFatal       = 1   // a cause that a resume will not fix
	exitUsage       = 2   // a malformed command line
	exitStuck       = 75  // no progress for the stall timeout; resumable when the journal is intact
	exitDeadline    = 124 // --max-duration reached with the output unfinished; resumable
	exitInterrupted = 130 // SIGINT
	exitTerminated  = 143 // SIGTERM, a graceful stop; resumable
)

// Causes the command cancels a run with.
var (
	errStuck    = errors.New("stuck: no progress within the stall timeout")
	errDeadline = errors.New("deadline: --max-duration reached")
)

// signalError is a run stopped by a signal.
type signalError struct{ sig os.Signal }

func (e *signalError) Error() string { return "stopped by " + e.sig.String() }

// outcome is how a command ended: its class and exit code.
type outcome struct {
	Class     string `json:"class"` // completed, fatal, usage, stuck, deadline, interrupted, terminated
	ExitCode  int    `json:"exit_code"`
	Message   string `json:"message,omitempty"`
	Resumable bool   `json:"resumable"` // swath resume DIR can continue it
	// Cause is the provider's error class when one is known (S3: auth,
	// access_denied, throttle, ...).
	Cause string `json:"cause,omitempty"`
}

// classify maps a command's error (and the cause its context was
// cancelled with) to an outcome. dir is the output directory, if any.
func classify(err, cause error, dir string) outcome {
	if err == nil {
		return outcome{Class: "completed", ExitCode: exitCompleted}
	}
	resumable := dir != "" && !dataset.Committed(dir) && hasJournal(dir)
	o := outcome{Message: err.Error(), Resumable: resumable}
	var u usageError
	var se *signalError
	switch {
	case errors.As(err, &u), errors.Is(err, flag.ErrHelp):
		o.Class, o.ExitCode, o.Resumable = "usage", exitUsage, false
	case errors.Is(cause, errStuck):
		o.Class, o.ExitCode = "stuck", exitStuck
	case errors.Is(cause, errDeadline):
		o.Class, o.ExitCode = "deadline", exitDeadline
	case errors.As(cause, &se) && se.sig == syscall.SIGTERM:
		o.Class, o.ExitCode = "terminated", exitTerminated
	case errors.As(cause, &se):
		o.Class, o.ExitCode = "interrupted", exitInterrupted
	default:
		o.Class, o.ExitCode, o.Resumable = "fatal", exitFatal, false
		var ce interface{ Class() string }
		if errors.As(err, &ce) {
			o.Cause = ce.Class()
		}
	}
	return o
}

// classified is an error with the provider's class of its cause.
type classified struct {
	error
	class string
}

func (c *classified) Class() string { return c.class }
func (c *classified) Unwrap() error { return c.error }

// exitError carries the exit code main uses.
type exitError struct {
	error
	code int
}

func (e *exitError) Unwrap() error { return e.error }

func hasJournal(dir string) bool {
	_, err := journal.Read(dir)
	return err == nil
}

// errorName is the outcome file written beside the report when a run does
// not complete.
const errorName = dataset.ErrorName

// writeOutcome records a failed run's outcome in DIR/error.json (atomic
// replace), so a supervisor never parses stderr; a completed run removes a
// stale one.
func writeOutcome(dir string, o outcome) {
	if dir == "" {
		return
	}
	path := filepath.Join(dir, errorName)
	if o.ExitCode == exitCompleted {
		_ = os.Remove(path)
		return
	}
	// Only into an output this tool owns: never into a directory it refused.
	if _, err := os.Stat(filepath.Join(dir, dataset.Workspace)); err != nil {
		return
	}
	writeFileAtomic(path, o)
}

func writeFileAtomic(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if os.WriteFile(tmp, append(b, '\n'), 0o644) == nil {
		_ = os.Rename(tmp, path)
	}
}

// progressName is the live progress file, inside the private workspace:
// rewritten (atomic replace) every progressEvery while a run goes.
var progressName = filepath.Join(dataset.Workspace, "progress.json")

const progressEvery = 2 * time.Second

// supervise runs list with the run's liveness owner: it rewrites the
// progress file, and cancels the run as stuck when Progress.Advance has not
// risen for stall (half of it earns a warning first). It is the only place
// that decides a run cannot go on; nothing in the pipeline gives up on a
// slow source.
func supervise(ctx context.Context, cancel context.CancelCauseFunc, pr *listing.Progress, dir string, stall time.Duration, warn func(string)) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(min(progressEvery, max(stall/4, 10*time.Millisecond)))
		defer t.Stop()
		last, since, warned := int64(-1), time.Now(), false
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case now := <-t.C:
				s := pr.Snapshot()
				if dir != "" {
					if _, err := os.Stat(filepath.Join(dir, dataset.Workspace)); err == nil {
						writeFileAtomic(filepath.Join(dir, progressName), s)
					}
				}
				if s.Advance != last {
					last, since, warned = s.Advance, now, false
					continue
				}
				if stall <= 0 {
					continue
				}
				idle := now.Sub(since)
				switch {
				case idle >= stall:
					warn(fmt.Sprintf("no progress for %s: stopping the run as stuck", idle.Round(time.Second)))
					cancel(errStuck)
					return
				case idle >= stall/2 && !warned:
					warned = true
					warn(fmt.Sprintf("no progress for %s (the run is stopped as stuck at %s)", idle.Round(time.Second), stall))
				}
			}
		}
	}()
	return func() { close(done); <-finished }
}
