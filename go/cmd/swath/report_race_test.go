// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/listing"
)

// TestConcurrentReportWriters (B3): the graceful and the forced account of
// an attempt may write one path at once. Each writer replaces the file
// through its own temporary file: every write succeeds, the file is always
// one writer's whole value, and no temporary file is left. Before, both
// went through path+".tmp": one truncated or renamed the other's file away
// (a write failed with ENOENT, or the file held a torn value).
func TestConcurrentReportWriters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")
	for round := 0; round < 50; round++ {
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v := map[string]string{"writer": strings.Repeat(string(rune('a'+w)), 64<<10)}
				errs <- dataset.ReplaceJSON(path, v, dataset.Durable)
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		var v map[string]string
		b, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(b, &v) != nil || len(v["writer"]) != 64<<10 || strings.Trim(v["writer"], v["writer"][:1]) != "" {
			t.Fatalf("round %d: torn file (%d bytes): %v", round, len(b), err)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*tmp*")); len(left) > 0 {
		t.Fatalf("temporary files left: %v", left)
	}
}

// TestOneTerminalReport (B3): an attempt's terminal report is written by
// the first of its writers; a later one (the forced exit after the graceful
// path wrote it, or the reverse) leaves it.
func TestOneTerminalReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	att := newAttempt()
	first := listing.Unstarted(nil, "", att, nil)
	first.Outcome.Status = "committed"
	second := listing.Unstarted(nil, "", att, nil)
	writeReport(path, first)
	writeReport(path, second)
	var r listing.Report
	readJSON(t, path, &r)
	if r.Outcome.Status != "committed" {
		t.Fatalf("the second writer replaced the attempt's report: %+v", r.Outcome)
	}
}

// TestOutcomeFilesDurable: error.json and the report are replaced at the
// Durable level (the temporary file synced before its rename), so a power
// loss after the command returns cannot lose the outcome a supervisor or
// journal.Witness reads; a completed run's removal of error.json is
// followed by a directory sync. SIGKILL (the crash matrix) cannot show
// this; the sync is observed through the faults hook.
func TestOutcomeFilesDurable(t *testing.T) {
	var mu sync.Mutex
	synced := map[string]bool{}
	defer faults.SetIO(func(op, name string, _ []byte) (int, error) {
		if op == "sync" {
			mu.Lock()
			synced[strings.SplitN(filepath.Base(name), ".", 3)[1]] = true
			mu.Unlock()
		}
		return 0, nil
	})()
	dir := t.TempDir()
	if err := writeOutcome(dir, outcome{ExitCode: exitStuck, Class: "stuck"}); err != nil {
		t.Fatal(err)
	}
	writeReport(filepath.Join(dir, "report.json"), listing.Report{})
	if !synced["error"] || !synced["report"] {
		t.Fatalf("synced before rename: %v, want error.json and report.json", synced)
	}
	if err := writeOutcome(dir, outcome{ExitCode: exitCompleted}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, errorName)); !os.IsNotExist(err) {
		t.Fatalf("a completed run left error.json: %v", err)
	}
}
