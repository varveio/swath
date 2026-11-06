// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"runtime/debug"
	"time"

	"github.com/varveio/swath/go/internal/source"
)

// ReportVersion is the run report's version.
const ReportVersion = 1

// Report is the one run report: run.json beside a committed dataset, and
// what -json prints, for every provider. It is operational: readers accept
// a dataset by its manifest, never by this report. It accounts for this
// invocation (rows it listed, requests it made) separately from the dataset
// (rows retained from an interrupted run, rows committed). Durations are
// integers with their unit in the name.
type Report struct {
	ReportVersion int   `json:"report_version"`
	Build         Build `json:"build"`
	// Attempt is the invocation this report accounts for (Config.Attempt).
	Attempt    *Attempt   `json:"attempt,omitempty"`
	Invocation Invocation `json:"invocation"`
	Outcome    Outcome    `json:"outcome"`
	Timings    Timings    `json:"timings"`
	Output     Output     `json:"output"`
	Resources  Resources  `json:"resources"`
	// Source is the provider's account of the listing, under its name.
	Source *SourceStats `json:"source,omitempty"`

	report *source.Report
}

// Attempt identifies one invocation: its report and the outcome the command
// records in the output (error.json) carry the same one, so a reader holding
// this invocation's report can tell a stale outcome file, left by an earlier
// attempt whose successor could not write its own, from a current one.
type Attempt struct {
	ID      string    `json:"id"`      // random, 128 bits, hex
	Started time.Time `json:"started"` // when the invocation began (UTC)
}

// Unstarted is the report of an invocation that stopped before a run began
// (a resume refused on its journal): what was asked, and the failure.
func Unstarted(argv []string, out string, attempt *Attempt, err error) Report {
	r := Report{ReportVersion: ReportVersion, Build: build(), Attempt: attempt,
		Invocation: Invocation{Argv: argv, Out: out}, Outcome: Outcome{Status: "failed"}}
	if err != nil {
		r.Outcome.Error = err.Error()
	}
	now := time.Now().UTC()
	r.Timings.StartedAt, r.Timings.FinishedAt = now, now
	if attempt != nil {
		r.Timings.StartedAt = attempt.Started
		r.Timings.WallMs = now.Sub(attempt.Started).Milliseconds()
	}
	return r
}

// Build names the binary.
type Build struct {
	Tool     string `json:"tool"` // "swath-go"
	Version  string `json:"version"`
	Revision string `json:"revision,omitempty"` // VCS revision, "-dirty" if modified
}

// Invocation is what was asked.
type Invocation struct {
	Argv          []string `json:"argv"`
	Source        string   `json:"source"`           // the root as recorded
	Out           string   `json:"out,omitempty"`    // the dataset directory; empty for a dry run
	Resume        bool     `json:"resume,omitempty"` // continued an interrupted run
	SchemaID      string   `json:"schema_id"`
	Fields        string   `json:"fields,omitempty"` // the metadata tier, for providers with tiers
	Workers       int      `json:"workers"`
	Encoders      int      `json:"encoders"`
	MemoryBytes   int64    `json:"memory_bytes"`
	PartSizeBytes int64    `json:"part_size_bytes"`
}

// Outcome is how the run ended.
type Outcome struct {
	// Status is "committed" (the dataset was published), "listed" (a dry
	// run finished) or "failed".
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	Complete   bool   `json:"complete"`              // the listing reached the end of its scope
	StopReason string `json:"stop_reason,omitempty"` // "completed", or why it stopped early
	// EntryErrors counts per-entry problems the provider counted (not fatal).
	EntryErrors int64 `json:"entry_errors"`
	// Class, ExitCode and Resumable are the command's account of how the
	// run ended (completed, fatal, stuck, deadline, interrupted,
	// terminated); Cause is the provider's class of a failure.
	Class     string `json:"class,omitempty"`
	ExitCode  int    `json:"exit_code"`
	Resumable bool   `json:"resumable"`
	Cause     string `json:"cause,omitempty"`
	// Unrecorded says why the output's outcome file does not hold this
	// outcome: it could not be written, or another process owns the output.
	Unrecorded string `json:"unrecorded,omitempty"`
}

// Timings of the run, wall and CPU.
type Timings struct {
	StartedAt         time.Time `json:"started_at"`
	ListingFinishedAt time.Time `json:"listing_finished_at,omitzero"`
	FinishedAt        time.Time `json:"finished_at"`
	WallMs            int64     `json:"wall_ms"`
	ListingMs         int64     `json:"listing_ms"` // to the end of the order stage
	UserCPUMs         int64     `json:"user_cpu_ms"`
	SysCPUMs          int64     `json:"sys_cpu_ms"`
}

// Output is the row and part accounting: this invocation versus the
// dataset.
type Output struct {
	RowsListed    int64 `json:"rows_listed"`    // listed and ordered by this invocation
	RowsRetained  int64 `json:"rows_retained"`  // kept from the interrupted run (resume)
	RowsCommitted int64 `json:"rows_committed"` // in the committed dataset (0 unless committed)
	// RowsDurable is the rows in parts that stay in the output: the
	// committed dataset's, or after a failure the journaled parts' (kept
	// for swath resume; 0 without a journal).
	RowsDurable   int64 `json:"rows_durable"`
	PartsWritten  int   `json:"parts_written"` // written by this invocation (after a failure: journaled, durable)
	PartsRetained int   `json:"parts_retained"`
	Bytes         int64 `json:"bytes"` // the dataset's part bytes
}

// Resources the run used. Wait times sum over goroutines.
type Resources struct {
	PeakRSSBytes     int64 `json:"peak_rss_bytes"`
	RowBudgetBytes   int64 `json:"row_budget_bytes"`   // rows in flight: reorder storage and encoder queues
	MaxBufferedBytes int64 `json:"max_buffered_bytes"` // peak reorder storage
	FrontierWaitNs   int64 `json:"frontier_wait_ns"`
	BudgetWaitNs     int64 `json:"budget_wait_ns"`
	// EncoderWaitNs is the time the order stage waited for an encoder (nil
	// without output).
	EncoderWaitNs *int64 `json:"encoder_wait_ns,omitempty"`
	Spill         *Spill `json:"spill,omitempty"` // nil: spill was off
	// Features are the pipeline's policies and whether this run had them
	// on; an off policy reports no counters, so a zero always means "on,
	// never fired" (docs/internals/counters.md).
	Features map[string]bool `json:"features"`
	// Peaks of the order stage's metadata: outstanding range nodes and
	// spill markers in memory (spill readers: Spill.PeakReaders).
	PeakNodes   int64 `json:"peak_nodes"`
	PeakMarkers int64 `json:"peak_markers"`
	// PartTimeCuts counts parts closed by the part interval, not by size:
	// with any, part boundaries depended on timing.
	PartTimeCuts int64 `json:"part_time_cuts"`
}

// SourceStats is the provider's statistics under its name ("s3", "local").
type SourceStats struct {
	Name  string `json:"name"`
	Stats any    `json:"stats"`
}

// Errors is the listing's per-entry error count.
func (r *Report) Errors() int64 { return r.Outcome.EntryErrors }

// SourceReport is the source's own report (nil before the listing ends, or
// for a source without one). Its Stats are the provider's own type.
func (r *Report) SourceReport() *source.Report { return r.report }

func (r *Report) setSource(sr source.Report) {
	r.report = &sr
	r.Invocation.Fields = sr.Fields
	r.Outcome.EntryErrors = sr.Errors
	r.Outcome.Complete, r.Outcome.StopReason = sr.Coverage.Complete, sr.Coverage.StopReason
	r.Source = &SourceStats{Name: sr.Name, Stats: sr.Stats}
}

// finish stamps the end time and the resources.
func (r *Report) finish() {
	now := time.Now()
	r.Timings.FinishedAt = now.UTC()
	r.Timings.WallMs = now.Sub(r.Timings.StartedAt).Milliseconds()
	u, sys := cpuTimes()
	r.Timings.UserCPUMs, r.Timings.SysCPUMs = u.Milliseconds(), sys.Milliseconds()
	r.Resources.PeakRSSBytes = peakRSS()
}

// BuildInfo is this binary's name, module version and VCS revision.
func BuildInfo() Build { return build() }

// build is this binary's name, module version and VCS revision.
func build() Build {
	b := Build{Tool: "swath-go", Version: "devel"}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return b
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		b.Version = v
	}
	dirty := false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			b.Revision = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if b.Revision != "" && dirty {
		b.Revision += "-dirty"
	}
	return b
}
