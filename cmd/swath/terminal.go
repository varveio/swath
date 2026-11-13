// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/listing"
)

// terminal is a run's one terminal result: frozen at its first
// classification (Finalize, before any account is written), with the
// report as last accounted under it. Every account derives from it: the
// report (--report, run.json, -json), error.json (outcomeOf the report),
// the outcome line on stderr, the exit code and a forced exit's writes; a
// stop cause that arrives later (the deadline passing while the run
// settles) cannot reclassify it. The commit (the manifest's installation)
// and its account are one step against a forced exit (commit, claim): the
// dataset is committed iff the result the process exits with says so, with
// two exceptions documented in go/docs/internals/durability.md ("The outcome
// of an attempt"). A forced exit that gets no snapshot within 2 s writes no
// outcome, although a manifest may be visible. A manifest renamed into place
// whose directory sync then fails stays visible under a failure result. Safe
// for concurrent use; a nil terminal holds nothing.
type terminal struct {
	ctx      context.Context // the run's: its cause is how it stopped
	dir      string
	provider func(error) string
	att      *listing.Attempt

	gate   sync.Mutex // held by commit, and by claim before mu
	forced bool       // under gate: a forced exit took the result; no commit follows

	mu  sync.Mutex
	err error    // as classified
	out *outcome // nil until classified
	rep []byte   // the last accounted report, JSON: a forced exit's copy
}

// classify freezes the classification of (sum, err) at the first call and
// returns it at every call.
func (t *terminal) classify(sum listing.Report, err error) (error, outcome) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.out == nil {
		_ = faults.At("classify") // a test hook: SWATH_TEST_HANG_AT holds the lock here
		err, out := classifyRun(t.ctx, t.dir, t.provider, sum, err)
		out.Attempt = t.att
		t.err, t.out = err, &out
	}
	return t.err, *t.out
}

// account applies the frozen classification to r and keeps r as the
// terminal report (listing.Config.Finalize).
func (t *terminal) account(r *listing.Report, err error) {
	_, out := t.classify(*r, err)
	account(&r.Outcome, out)
	b, jerr := json.Marshal(r)
	t.mu.Lock()
	defer t.mu.Unlock()
	if jerr == nil {
		t.rep = b
	}
}

// errForced refuses a commit once a forced exit took the run's result.
var errForced = errors.New("not committed: the forced exit took the run's result first")

// commit runs step, the manifest's installation and the committed report's
// account (listing.Config.Commit), unless a forced exit took the run's
// result first; a forced exit that comes during step waits for it (claim).
func (t *terminal) commit(step func() error) error {
	t.gate.Lock()
	defer t.gate.Unlock()
	if t.forced {
		return errForced
	}
	return step()
}

// snapshot is a forced exit's one view of the terminal result: every file
// it writes and its exit code derive from it.
type snapshot struct {
	out outcome
	rep []byte // the run's last accounted report (nil: none)
	run bool   // out is the run's own classification, not the forced one
}

// report is the snapshot's accounted report, once there is one.
func (s snapshot) report() (listing.Report, bool) {
	var r listing.Report
	if s.rep == nil || json.Unmarshal(s.rep, &r) != nil {
		return listing.Report{}, false
	}
	return r, true
}

// claim takes a forced exit's snapshot: the frozen result, or, when there is
// none, forced() frozen now, so neither a commit nor a classification that
// comes later makes another. A commit or classification under way is waited
// for, at most wait; ok false then: no snapshot (the caller writes no
// outcome), and the claim completes whenever the holder lets go, with late
// called on the snapshot then (the run's own accounts derive from it).
func (t *terminal) claim(forced func() outcome, wait time.Duration, late func(snapshot)) (s snapshot, ok bool) {
	if t == nil {
		return snapshot{}, false
	}
	ch := make(chan snapshot, 1)
	go func() {
		t.gate.Lock()
		defer t.gate.Unlock()
		t.forced = true
		t.mu.Lock()
		defer t.mu.Unlock()
		s := snapshot{rep: t.rep, run: t.out != nil}
		if t.out == nil {
			o := forced()
			o.Attempt = t.att
			t.err, t.out = errors.New(o.Message), &o
		}
		s.out = *t.out
		ch <- s
	}()
	select {
	case s := <-ch:
		return s, true
	case <-time.After(wait):
		if late != nil {
			go func() { late(<-ch) }()
		}
		return snapshot{}, false
	}
}

// outcomeOf is the outcome file's view (error.json, the stderr outcome
// line) of an accounted report: the same class, exit code, resumability,
// cause, message and attempt.
func outcomeOf(r listing.Report) outcome {
	return outcome{Class: r.Outcome.Class, ExitCode: r.Outcome.ExitCode, Message: r.Outcome.Error,
		Resumable: r.Outcome.Resumable, Cause: r.Outcome.Cause, StopSource: r.Outcome.StopSource, Attempt: r.Attempt}
}
