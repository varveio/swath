// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/varveio/swath/go/internal/listing"
)

// terminal is a run's one terminal result: frozen at its first
// classification (Finalize, before any account is written), with the
// report as last accounted under it. Every account derives from it: the
// report (--report, run.json, -json), error.json (outcomeOf the report),
// the outcome line on stderr, the exit code and a forced exit's writes; a
// stop cause that arrives later (the deadline passing while the run
// settles) cannot reclassify it. Safe for concurrent use; a nil terminal
// holds nothing.
type terminal struct {
	ctx      context.Context // the run's: its cause is how it stopped
	dir      string
	provider func(error) string
	att      *listing.Attempt

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

// frozen is the classification, once made.
func (t *terminal) frozen() (outcome, bool) {
	if t == nil {
		return outcome{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.out == nil {
		return outcome{}, false
	}
	return *t.out, true
}

// report is the last accounted report, once there is one.
func (t *terminal) report() (listing.Report, bool) {
	if t == nil {
		return listing.Report{}, false
	}
	t.mu.Lock()
	b := t.rep
	t.mu.Unlock()
	var r listing.Report
	if b == nil || json.Unmarshal(b, &r) != nil {
		return listing.Report{}, false
	}
	return r, true
}

// outcomeOf is the outcome file's view (error.json, the stderr outcome
// line) of an accounted report: the same class, exit code, resumability,
// cause, message and attempt.
func outcomeOf(r listing.Report) outcome {
	return outcome{Class: r.Outcome.Class, ExitCode: r.Outcome.ExitCode, Message: r.Outcome.Error,
		Resumable: r.Outcome.Resumable, Cause: r.Outcome.Cause, StopSource: r.Outcome.StopSource, Attempt: r.Attempt}
}
