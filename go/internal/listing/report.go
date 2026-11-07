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
	// ArgsHash is the digest of the run's frozen identity
	// (journal.Identity.Hash): the same for a run and its resumes; empty
	// when the run stopped before its source opened.
	ArgsHash string `json:"args_hash,omitempty"`
	// Cost is this invocation's HTTP requests and their estimated price
	// (nil for a source that makes no requests, or before it opened).
	Cost *Cost `json:"cost,omitempty"`
	// Efficiency is this invocation's throughput and request economy.
	Efficiency *Efficiency `json:"efficiency,omitempty"`

	report    *source.Report
	finalized bool            // Config.Finalize has accounted it
	src       any             // the opened source, read by finish for the request account
	live      source.Requests // its request account at the end
}

// Requests is the source's request account when the report was finished
// (zero for a source that makes no requests).
func (r *Report) Requests() source.Requests { return r.live }

// Cost is an invocation's request account: every HTTP attempt it sent
// (pages, probes, client retries, engine requeues) and, where the service's
// reference tariff is known, an estimate labelled with its basis.
type Cost struct {
	APICalls int64 `json:"api_calls"`
	// CostUSD is APICalls at Basis's rate; null without a known tariff (an
	// S3-compatible endpoint): an unlabelled AWS figure would mislead.
	CostUSD *float64   `json:"cost_usd"`
	Basis   *CostBasis `json:"basis"`
}

// CostBasis names the rate an estimate was derived from: a single-region
// reference list rate, so a reader can rescale it.
type CostBasis struct {
	RatePer1kUSD float64 `json:"rate_per_1k_usd"`
	Source       string  `json:"source"` // aws-list-reference-rate, gcs-class-a-reference-rate
}

// Efficiency is an invocation's throughput and request economy, over this
// invocation's own rows (rows retained from an interrupted run excluded).
type Efficiency struct {
	// KeysPerSec is output.rows_listed over timings.listing_ms: listing and
	// ordering, not publication or the time before the run started.
	KeysPerSec float64 `json:"keys_per_sec"`
	// APICallsPer1kObjects is cost.api_calls per 1,000 rows listed; null
	// with no row listed.
	APICallsPer1kObjects *float64 `json:"api_calls_per_1k_objects"`
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
		Invocation: Invocation{Argv: argv, Out: out}, Outcome: Outcome{Status: StatusFailed}}
	if err != nil {
		r.Outcome.Error = err.Error()
	}
	now := time.Now().UTC()
	r.Timings.StartedAt, r.Timings.FinishedAt = now, now
	if attempt != nil {
		r.Timings.StartedAt = attempt.Started
		r.Timings.WallMs = now.Sub(attempt.Started).Milliseconds()
		r.Timings.SessionMs = r.Timings.WallMs
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
	Argv           []string `json:"argv"`
	Source         string   `json:"source"`           // the root as recorded
	Out            string   `json:"out,omitempty"`    // the dataset directory; empty for a dry run
	Resume         bool     `json:"resume,omitempty"` // continued an interrupted run
	SchemaID       string   `json:"schema_id"`
	Fields         string   `json:"fields,omitempty"` // the metadata tier, for providers with tiers
	Workers        int      `json:"workers"`
	Encoders       int      `json:"encoders"`
	RowBudgetBytes int64    `json:"row_budget_bytes"`
	PartSizeBytes  int64    `json:"part_size_bytes"`
}

// The outcome vocabulary, in one place. Three separate dimensions:
//   - Status: what became of the output (publication);
//   - Class (with ExitCode and Resumable): how the invocation ended, the
//     command's classification; StopReason follows it for a run that did
//     not commit, and holds the coverage's reason for one that did;
//   - Cause: the underlying failure's class, the provider's (objhttp.Class:
//     auth, throttle, ...) or the command's (stuck_*, no_space, protocol).
//
// Error is the human message; the outcome file (error.json) carries the
// same text as "message".
const (
	StatusCommitted = "committed" // the dataset was published
	StatusListed    = "listed"    // a dry run finished
	StatusWritten   = "written"   // a text run finished (Output.Text)
	StatusStopped   = "stopped"   // a text stream whose reader closed it (exit 0)

	// StopDownstreamClosed is the stop reason of a text stream whose
	// reader closed it.
	StopDownstreamClosed = "downstream_closed"
	StatusFailed         = "failed" // nothing was published

	ClassCompleted   = "completed"
	ClassFatal       = "fatal"
	ClassIncomplete  = "incomplete"
	ClassUsage       = "usage"
	ClassRefused     = "refused"
	ClassDiskFull    = "disk_full"
	ClassStuck       = "stuck"
	ClassDeadline    = "deadline"
	ClassInterrupted = "interrupted"
	ClassTerminated  = "terminated"
)

// Outcome is how the run ended.
type Outcome struct {
	// Status is StatusCommitted, StatusWritten, StatusListed, StatusStopped or
	// StatusFailed.
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	Complete   bool   `json:"complete"`              // the listing reached the end of its scope
	StopReason string `json:"stop_reason,omitempty"` // "completed", or why it stopped early
	// StopSource is what stopped a run that was stopped: sigint, sigterm,
	// liveness_watchdog (--stall-timeout), timebox (--max-duration), with
	// "+forced_exit" when it did not stop within the shutdown grace; empty
	// for a run that ended on its own.
	StopSource string `json:"stop_source,omitempty"`
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
	StartedAt time.Time `json:"started_at"`
	// ListingDoneAt is when the provider finished (every range listed, none
	// in flight); ListingFinishedAt when the order stage ended, its drain
	// handed on: the drain's tail lies between (Resources.Drain.TailNs).
	ListingDoneAt     time.Time `json:"listing_done_at,omitzero"`
	ListingFinishedAt time.Time `json:"listing_finished_at,omitzero"`
	FinishedAt        time.Time `json:"finished_at"`
	WallMs            int64     `json:"wall_ms"` // from started_at (the run's start, after the command line was read) to finished_at
	// SessionMs is the whole invocation: from attempt.started (the command
	// began) to finished_at, the credential lookup, a resume's
	// verification and publication included.
	SessionMs int64 `json:"session_ms"`
	ListingMs int64 `json:"listing_ms"` // to the end of the order stage
	// UserCPUMs and SysCPUMs are the process's CPU times: absent (nil)
	// where the platform does not measure them (Resources.RusageAvailable).
	UserCPUMs *int64 `json:"user_cpu_ms,omitempty"`
	SysCPUMs  *int64 `json:"sys_cpu_ms,omitempty"`
}

// Output is the row and part accounting: this invocation versus the
// dataset.
type Output struct {
	RowsListed    int64 `json:"rows_listed"`    // listed and ordered (a dry run: counted) by this invocation, after the filters
	RowsFiltered  int64 `json:"rows_filtered"`  // listed by this invocation and dropped by the filters (--include, ...)
	RowsRetained  int64 `json:"rows_retained"`  // kept from the interrupted run (resume)
	RowsCommitted int64 `json:"rows_committed"` // in the committed dataset (0 unless committed)
	// RowsDurable is the rows in parts that stay in the output: the
	// committed dataset's, or after a failure the journaled parts' (kept
	// for swath resume; 0 without a journal).
	RowsDurable   int64 `json:"rows_durable"`
	PartsWritten  int   `json:"parts_written"` // written by this invocation (after a failure: journaled, durable)
	PartsRetained int   `json:"parts_retained"`
	Bytes         int64 `json:"bytes"` // the dataset's part bytes
	// Text is a text run's output (nil for a dataset or a dry run).
	Text *TextOutput `json:"text,omitempty"`
}

// TextOutput is a text run's destination and what reached it. A run that
// did not complete published no file (its temporary was removed); on a
// stream, Rows and Bytes are what was written before it stopped.
type TextOutput struct {
	Format      string `json:"format"`      // tsv, jsonl, table
	Destination string `json:"destination"` // the file, or "-" for stdout
	Compression string `json:"compression"` // none, gzip, zstd
	Rows        int64  `json:"rows"`
	Bytes       int64  `json:"bytes"` // delivered, compressed
	// SinkWaitNs is the time the order stage spent in the text sink: with
	// one formatter, formatting rows and handing the bytes on; with more,
	// handing batches to them (waiting while their queue is full).
	// OutputWaitNs is the time spent handing bytes to the compressor, or
	// to the file or stream when uncompressed: by the order stage with one
	// formatter, by the sequencer behind them with more (textout.Stats).
	// Formatters is the goroutines formatting rows (1: the order stage).
	SinkWaitNs   int64 `json:"sink_wait_ns"`
	OutputWaitNs int64 `json:"output_wait_ns"`
	Formatters   int   `json:"formatters"`
	// Compressor is the compression's work (absent uncompressed).
	Compressor *TextCompressor `json:"compressor,omitempty"`
}

// TextCompressor is a compressed text output's compressor: the bytes in
// (formatted rows) and out (delivered), and the zstd jobs compressed at
// once with why that many (gzip is one stream).
type TextCompressor struct {
	BytesIn    int64  `json:"bytes_in"`
	BytesOut   int64  `json:"bytes_out"`
	Jobs       int    `json:"jobs"`
	JobsReason string `json:"jobs_reason"` // a Jobs* reason
}

// Why a text output's compressor runs the jobs it does
// (TextCompressor.JobsReason).
const (
	JobsAsked     = "encoders"   // --encoders (Limits.Encoders) set it
	JobsCores     = "cores"      // GOMAXPROCS: the budget carries more
	JobsRowBudget = "row_budget" // the row budget's allowance carries fewer than GOMAXPROCS (admitTextJobs)
	JobsGzip      = "gzip"       // gzip: one stream
)

// Resources the run used. Wait times sum over goroutines.
type Resources struct {
	// PeakRSSBytes is the process's peak resident set (Windows: peak
	// working set): absent (nil) where it is not measured.
	PeakRSSBytes *int64 `json:"peak_rss_bytes,omitempty"`
	// RusageAvailable says whether this platform measured the CPU times and
	// peak RSS; false leaves them out rather than reporting zeros.
	RusageAvailable  bool  `json:"rusage_available"`
	RowBudgetBytes   int64 `json:"row_budget_bytes"`   // rows in flight: reorder storage and encoder queues
	MaxBufferedBytes int64 `json:"max_buffered_bytes"` // peak reorder storage
	// PeakHeapLiveBytes is the peak live heap while listing (the bytes a
	// garbage collection marked reachable, sampled every 100 ms): beside
	// row_budget_bytes, the memory the budget does not see.
	PeakHeapLiveBytes int64 `json:"peak_heap_live_bytes"`
	// PeakAheadRows is the most rows listed and not yet handed on at once
	// (ahead of the publish frontier, in memory or spilled);
	// FrontierStuckMaxNs the longest interval in which the order stage
	// handed nothing on. Both 0 for a dry run, which does not order.
	PeakAheadRows      int64 `json:"peak_ahead_rows"`
	FrontierStuckMaxNs int64 `json:"frontier_stuck_max_ns"`
	FrontierWaitNs     int64 `json:"frontier_wait_ns"`
	BudgetWaitNs       int64 `json:"budget_wait_ns"`
	// EncoderWaitNs is the time the order stage waited for an encoder (nil
	// without output).
	EncoderWaitNs *int64 `json:"encoder_wait_ns,omitempty"`
	Spill         *Spill `json:"spill,omitempty"` // nil: spill was off
	// Drain is the order stage's drain (nil for a dry run).
	Drain *Drain `json:"drain,omitempty"`
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
	// PartRowCuts counts parts closed by --part-rotation-max-rows before
	// their size target (deterministic, unlike time cuts).
	PartRowCuts int64 `json:"part_row_cuts"`
}

// Drain is the order stage's drain after listing and the time it took.
// Durations sum over goroutines where several wait.
type Drain struct {
	// TailNs is from listing_done_at to the drain's last row: the part of
	// the run in which only the drain was left (0: listing never finished).
	TailNs int64 `json:"tail_ns"`
	// The workers' idle time (nothing queued to list; read-ahead waits
	// included), summed over workers, before and after listing_done_at.
	WorkerIdleBeforeNs int64 `json:"worker_idle_before_listing_done_ns"`
	WorkerIdleAfterNs  int64 `json:"worker_idle_after_listing_done_ns"`
	// The drainer's time beside frontier_wait_ns: reading spilled frames
	// itself (decoding included, and counted again in ReplayDecodeNs),
	// waiting for a frame the read-ahead was reading, the order guard, and
	// handing batches to the sink (encoder_wait_ns included).
	ReplayReadNs   int64 `json:"replay_read_ns"`
	ReplayDecodeNs int64 `json:"replay_decode_ns"`
	AheadWaitNs    int64 `json:"ahead_wait_ns"`
	OrderCheckNs   int64 `json:"order_check_ns"`
	SinkWriteNs    int64 `json:"sink_write_ns"`
	// ReadAhead is the spill read-ahead (nil: spill off).
	ReadAhead *ReadAhead `json:"read_ahead,omitempty"`
	// Segments is the segment-parallel replay after listing (nil without
	// a dataset).
	Segments *Segments `json:"segments,omitempty"`
	// The Parquet encoders admitted at the start and once listing ended,
	// and why the second is what it is (EncoderAdmission*); absent without
	// a dataset.
	EncodersAtStart      int    `json:"encoders_at_start,omitempty"`
	EncodersAfterListing int    `json:"encoders_after_listing,omitempty"`
	EncoderAdmission     string `json:"encoder_admission,omitempty"`
}

// Why the encoders after listing are what they are (Drain.EncoderAdmission).
const (
	AdmissionListingDone = "listing_done" // raised when the provider finished (admitAfterListing)
	AdmissionExplicit    = "explicit"     // --encoders was given: never raised
	AdmissionAtCap       = "at_cap"       // the start already admitted the most the host carries
	AdmissionNotReached  = "not_reached"  // listing never finished (a failed or stopped run)
)

// ReadAhead counts the spill read-ahead: frames idle workers read and
// decoded before the drainer reached them, and their rows; frames planned
// that the drainer took over unread and frames it reached with nothing
// planned (both read by the drainer itself); the drainer's waits for a
// frame being read; workers' waits at the read-ahead's share of the row
// budget; and the workers' read and decode time.
type ReadAhead struct {
	Frames        int64 `json:"frames"`
	Rows          int64 `json:"rows"`
	DrainerFrames int64 `json:"drainer_frames"`
	Unplanned     int64 `json:"unplanned_frames"`
	Waits         int64 `json:"drainer_waits"`
	CappedWaits   int64 `json:"capped_waits"`
	ReadNs        int64 `json:"read_ns"`
	DecodeNs      int64 `json:"decode_ns"`
	// MaxReads is the reads allowed at once (8, at most one fewer than
	// GOMAXPROCS): 0 is off, the drainer replaying alone (one processor).
	MaxReads int `json:"max_reads"`
}

// Segments is the segment-parallel replay: once listing was done, the rest
// of the stream cut into parts by the order stage and written by several
// workers at once (order.SegmentStats).
type Segments struct {
	// Reason: engaged, one_processor, one_encoder, single_part,
	// not_planned or listing_active.
	Reason   string `json:"reason"`
	Segments int    `json:"segments"`
	Workers  int    `json:"workers"`
	Rows     int64  `json:"rows"`
	MaxRows  int64  `json:"max_rows"`
	Seams    int64  `json:"seam_checks"`
	TimeNs   int64  `json:"time_ns"`     // summed over segments, open to close
	MaxNs    int64  `json:"max_time_ns"` // the longest segment
	PlanNs   int64  `json:"plan_ns"`     // cutting the rest into segments (boundary frames decoded)
	// The segments' time by what it did: reading spill frames (DecodeNs
	// of it decoding), handing batches to their parts (WriteNs: the
	// encoders' queue waits included); the rest of time_ns is the order
	// guard and opening parts.
	ReadNs   int64 `json:"read_ns"`
	DecodeNs int64 `json:"decode_ns"`
	WriteNs  int64 `json:"write_ns"`
	// MaxWorkers is the cap on workers (listing.segmentWorkers).
	MaxWorkers int `json:"max_workers"`
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
	r.setRusage(rusage())
	if r.Attempt != nil {
		r.Timings.SessionMs = now.Sub(r.Attempt.Started).Milliseconds()
	}
	r.requests()
}

// setRusage records the process's measurements, or that there are none.
func (r *Report) setRusage(user, sys time.Duration, peak int64, ok bool) {
	r.Resources.RusageAvailable = ok
	if !ok {
		r.Timings.UserCPUMs, r.Timings.SysCPUMs, r.Resources.PeakRSSBytes = nil, nil, nil
		return
	}
	u, s := user.Milliseconds(), sys.Milliseconds()
	r.Timings.UserCPUMs, r.Timings.SysCPUMs, r.Resources.PeakRSSBytes = &u, &s, &peak
}

// requests fills Cost and Efficiency from the source's request account.
func (r *Report) requests() {
	rq, ok := r.src.(source.Requested)
	if !ok {
		return
	}
	r.live = rq.Requests()
	n := r.live.Attempts
	c := &Cost{APICalls: n}
	if p, ok := r.src.(source.Priced); ok {
		if rate, basis := p.Tariff(); basis != "" {
			usd := float64(n) / 1000 * rate
			c.CostUSD, c.Basis = &usd, &CostBasis{RatePer1kUSD: rate, Source: basis}
		}
	}
	r.Cost = c
	e := &Efficiency{}
	if ms := r.Timings.ListingMs; ms > 0 {
		e.KeysPerSec = float64(r.Output.RowsListed) * 1000 / float64(ms)
	}
	if rows := r.Output.RowsListed; rows > 0 {
		v := float64(n) * 1000 / float64(rows)
		e.APICallsPer1kObjects = &v
	}
	r.Efficiency = e
}

// BuildInfo is this binary's name, module version and VCS revision.
func BuildInfo() Build { return build() }

// revision is the commit the binary was built from, set at link time by
// go/build.sh and CI (-ldflags -X ...listing.revision=...); empty, the Go
// toolchain's VCS stamp is used, which a build in a git worktree may lack.
var revision string

// build is this binary's name, module version and VCS revision.
func build() Build {
	b := Build{Tool: "swath-go", Version: "devel", Revision: revision}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return b
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		b.Version = v
	}
	if b.Revision != "" {
		return b
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
