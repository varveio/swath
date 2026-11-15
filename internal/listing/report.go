// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"time"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/jsontime"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/totals"
)

// ReportVersion is the run report's schema version, the report_version
// field: it covers the stable core, not Diagnostics.
const ReportVersion = 1

// Report is the one run report: run.json beside a committed dataset, and
// what --report writes, for every provider. It is operational: a dataset
// is read by its manifest, never by this report. It accounts for this
// invocation (rows it listed, requests it made) separately from the dataset
// (rows retained from an interrupted run, rows committed). Durations are
// integers with their unit in the name. Its members are its stable core,
// but for Diagnostics.
type Report struct {
	ReportVersion int   `json:"report_version"`
	Build         Build `json:"build"`
	// Attempt is the invocation this report accounts for (Config.Attempt).
	Attempt *Attempt `json:"attempt,omitempty"`
	// Run is the run the attempt belongs to (nil when the attempt stopped
	// before its output was opened).
	Run        *RunInfo   `json:"run,omitempty"`
	Invocation Invocation `json:"invocation"`
	Outcome    Outcome    `json:"outcome"`
	Timings    Timings    `json:"timings"`
	Process    Process    `json:"process"`
	Output     Output     `json:"output"`
	// ArgsHash is the digest of the run's frozen identity
	// (journal.Identity.Hash): the same for a run and its resumes; empty
	// when the run stopped before its source opened.
	ArgsHash string `json:"args_hash,omitempty"`
	// Requests is this invocation's HTTP account (nil for a source that
	// makes no requests, or before it opened).
	Requests *Requests `json:"requests,omitempty"`
	// Cost is the estimated price of Requests.HTTPAttempts (nil with
	// Requests).
	Cost *Cost `json:"cost,omitempty"`
	// Diagnostics is the report's diagnostics member: embedded, so its
	// fields read as the report's own (r.Resources), and written as one
	// JSON member, "diagnostics".
	Diagnostics `json:"diagnostics"`

	report    *source.Report
	sampler   *sampler        // the history's, stopped by finish
	finalized bool            // Config.Finalize has accounted it
	src       any             // the opened source, read by finish for the request account
	live      source.Requests // its request account at the end
}

// Diagnostics is everything in the report that describes how the
// pipeline ran rather than what the run did: the sort buffer's sizing, the
// seed, the text sink, the resources, the provider's counters, the
// efficiency, the history and the profiles. Its names follow the
// algorithm and may change in any release, without a new ReportVersion
// (docs/internals/counters.md).
type Diagnostics struct {
	// SortBuffer is how invocation.sort_buffer_bytes was chosen.
	SortBuffer RowBudget `json:"sort_buffer"`
	// SeedMs is the range engine's seed, from its first delimiter request
	// to its tiles (inside timings.listing_ms; absent where no seed ran: a
	// local source, a seed turned off, a warm start).
	SeedMs *int64 `json:"seed_ms,omitempty"`
	// Text is a text run's sink (nil for a dataset or a dry run).
	Text      *TextSink `json:"text,omitempty"`
	Resources Resources `json:"resources"`
	// Source is the provider's account of the listing, under its name.
	Source *SourceStats `json:"source,omitempty"`
	// Efficiency is this invocation's throughput and request economy.
	Efficiency *Efficiency `json:"efficiency,omitempty"`
	// History is the run's progress, sampled (at most HistoryCap samples;
	// nil for a run that stopped before listing).
	History *History `json:"history,omitempty"`
	// Profiles is the development profiles this run writes
	// (SWATH_CPUPROFILE, SWATH_MEMPROFILE; nil when neither is set).
	Profiles *Profiles `json:"profiles,omitempty"`
}

// Live is the source's request account when the report was finished
// (zero for a source that makes no requests).
func (r *Report) Live() source.Requests { return r.live }

// Profiles is the development profiles a run writes (SWATH_CPUPROFILE,
// SWATH_MEMPROFILE), split at the end of listing so the listing and the
// drain's tail after it are profiled apart. Diagnostics only: the caller
// (cmd/swath) fills it.
type Profiles struct {
	// PhaseBoundaryAt is the logical boundary between the phases: the
	// event timings.provider_finished_at records (the provider finished), taken
	// microseconds after it. Absent when listing never finished (a failed
	// or stopped run, a dry run): the listing-phase files then hold the
	// whole run and no tail file is written.
	PhaseBoundaryAt jsontime.Time `json:"phase_boundary_at,omitzero"`
	// CPUCutDelayNs is from the boundary to the listing phase's CPU
	// profile's stop (the phase switch runs on a goroutine of its own): the
	// listing file holds this much of the tail too. CPUGapNs is from that
	// stop to the tail's CPU profile's start, which neither covers: Go runs
	// one CPU profile at a time, and the listing phase's is written out
	// before the tail's starts. Both absent until the switch has run, the
	// gap also when the tail's profile could not start.
	CPUCutDelayNs *int64 `json:"cpu_cut_delay_ns,omitempty"`
	CPUGapNs      *int64 `json:"cpu_gap_ns,omitempty"`
	// MemProfileRate is runtime.MemProfileRate during the run (bytes
	// allocated per sample, 0: none sampled; absent without a memory
	// profile).
	MemProfileRate *int `json:"mem_profile_rate,omitempty"`
	// Files are the profile files, in the order cpu_listing, cpu_tail,
	// mem_listing, mem_exit.
	Files []ProfileFile `json:"files"`
}

// ProfileFile is one profile file: its Kind (cpu_listing: the CPU profile
// from the run's start to the phase switch, or to the end without one;
// cpu_tail: from the switch to the end; mem_listing, mem_exit: the
// allocation profile, cumulative from the start, as of the latest garbage
// collection before the switch and before the end), its Path, and its
// Status: planned (to be written later in the run: run.json, written at
// the commit, names the tail's and the exit's files so), written, or
// failed, with the Error.
type ProfileFile struct {
	Kind   string `json:"kind"`
	Path   string `json:"path"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Requests is an invocation's HTTP account. HTTPAttempts is the one
// request count: every HTTP attempt sent (pages, probes, client retries,
// engine requeues); Retries, Throttles and Timeouts are the attempts among
// them repeated after a retryable failure, answered with a throttle, and
// past the per-request timeout (retried or not).
type Requests struct {
	HTTPAttempts int64 `json:"http_attempts"`
	Retries      int64 `json:"retries"`
	Throttles    int64 `json:"throttles"`
	Timeouts     int64 `json:"timeouts"`
	// FailureWarnings counts the persistent-failure warnings printed on
	// stderr: requests failing with one cause, none answered, for 10 s.
	FailureWarnings int64 `json:"failure_warnings"`
	// LogDropped counts the stderr lines (the request log, mostly) dropped
	// because stderr did not drain.
	LogDropped int64 `json:"log_dropped"`
}

// Cost is the estimated price of an invocation's HTTP attempts
// (Requests.HTTPAttempts), where the service's reference tariff is known,
// labelled with its basis.
type Cost struct {
	// CostUSD is requests.http_attempts at Basis's rate; null without a
	// known tariff (an S3-compatible endpoint): an unlabelled AWS figure
	// would mislead.
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
	// KeysPerSec is output.rows_emitted over timings.listing_ms: listing and
	// ordering, not publication or the time before the run started.
	KeysPerSec float64 `json:"keys_per_sec"`
	// HTTPAttemptsPer1kRows is requests.http_attempts per 1,000 rows
	// fetched (before the filters: what the requests returned); null with
	// no row fetched.
	HTTPAttemptsPer1kRows *float64 `json:"http_attempts_per_1k_rows"`
}

// Attempt identifies one invocation: its report and the outcome the command
// records in the output (error.json) carry the same one, so a reader holding
// this invocation's report can tell a stale outcome file, left by an earlier
// attempt whose successor could not write its own, from a current one.
type Attempt struct {
	ID        string        `json:"id"`         // random, 128 bits, hex
	StartedAt jsontime.Time `json:"started_at"` // when the invocation began
}

// RunInfo identifies the run an attempt belongs to: the same for the run and
// every resume of it. Its ID is the journal's run UUID (a dataset in a
// bucket: the RUN of its data/RUN/ATTEMPT/ parts); a run without a journal
// is its one attempt, and has the attempt's ID.
type RunInfo struct {
	ID string `json:"id"`
}

// Unstarted is the report of an invocation that stopped before a run began
// (a resume refused on its journal): what was asked, and the failure.
func Unstarted(argv []string, out string, attempt *Attempt, err error) Report {
	r := Report{ReportVersion: ReportVersion, Build: buildinfo.Read(), Attempt: attempt,
		Invocation: Invocation{Argv: registry.RedactArgv(argv), Out: out}, Outcome: Outcome{Status: StatusFailed}}
	if err != nil {
		r.Outcome.Error = err.Error()
	}
	now := time.Now()
	r.Timings.StartedAt, r.Timings.FinishedAt = jsontime.Of(now), jsontime.Of(now)
	if attempt != nil {
		r.Timings.StartedAt = attempt.StartedAt
		r.Timings.WallMs = now.Sub(attempt.StartedAt.Time).Milliseconds()
		r.Timings.SessionMs = r.Timings.WallMs
	}
	return r
}

// Build names the binary.
type Build = buildinfo.Build

// Invocation is what was asked.
type Invocation struct {
	Argv           []string `json:"argv"`
	Source         string   `json:"source"`              // the root as recorded
	Narrowing      string   `json:"narrowing,omitempty"` // how the includes narrowed source (filter.Narrowed, or why not)
	Out            string   `json:"out,omitempty"`       // the dataset directory; empty for a dry run
	Resume         bool     `json:"resume,omitempty"`    // continued an interrupted run
	SchemaID       string   `json:"schema_id"`
	Metadata       string   `json:"metadata,omitempty"` // the metadata tier (--metadata), for providers with tiers
	Workers        int      `json:"workers"`
	Encoders       int      `json:"encoders"`
	RowBudgetBytes int64    `json:"sort_buffer_bytes"` // --sort-buffer; how it was chosen: Diagnostics.SortBuffer
	PartSizeBytes  int64    `json:"part_size_bytes"`
	Prior          *Prior   `json:"prior,omitempty"` // the --seed-from prior the range engine started from
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
	// ClassUnresolved: a dataset in a bucket whose manifest's creation
	// could not be confirmed or refuted; everything is kept, and a resume
	// creates the same manifest again.
	ClassUnresolved = "unresolved"
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
	// liveness_watchdog (--stall-timeout), timebox (--max-duration),
	// spill_wait (--spill-wait-timeout), with "+forced_exit" when it did
	// not stop within the shutdown grace; empty for a run that ended on its
	// own.
	StopSource string `json:"stop_source,omitempty"`
	// Unreadable counts the entries that could not be read: the rows and
	// totals leave them out, although Complete may be true (the manifest's
	// coverage.unreadable holds the same count).
	Unreadable int64 `json:"unreadable"`
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

// Timings of the run, wall and CPU. The phase times: listing_ms, drain_ms
// and publication_ms follow one another (each starts where the one before
// ended); seed_ms overlaps listing_ms, at its start; wall_ms holds them all
// and the setup before listing (credentials, the journal, a resume's
// verification); session_ms holds wall_ms.
type Timings struct {
	StartedAt jsontime.Time `json:"started_at"`
	// ProviderFinishedAt is when the provider finished (every range listed,
	// none in flight); ListingFinishedAt when the order stage ended, its
	// drain handed on: the drain's tail lies between
	// (Resources.Drain.TailNs).
	ProviderFinishedAt jsontime.Time `json:"provider_finished_at,omitzero"`
	ListingFinishedAt  jsontime.Time `json:"listing_finished_at,omitzero"`
	FinishedAt         jsontime.Time `json:"finished_at"`
	WallMs             int64         `json:"wall_ms"` // from started_at (the run's start, after the command line was read) to finished_at
	// SessionMs is the whole invocation: from attempt.started_at (the command
	// began) to finished_at, the credential lookup, a resume's
	// verification and publication included.
	SessionMs int64 `json:"session_ms"`
	// ListingMs runs from started_at to listing_finished_at, the end of the
	// order stage: every row listed and handed on in order (to the encoders'
	// queues, the text output, or counted). It includes the setup and the
	// order stage's drain after the last request.
	ListingMs int64 `json:"listing_ms"`
	// DrainMs runs from listing_finished_at until the encoders have written
	// and closed the last part; PublicationMs from there until the manifest
	// is installed (the commit). Both absent for a run that commits no
	// dataset.
	DrainMs       *int64 `json:"drain_ms,omitempty"`
	PublicationMs *int64 `json:"publication_ms,omitempty"`
	// UserCPUMs and SysCPUMs are the process's CPU times: absent (nil)
	// where the platform does not measure them (Resources.RusageAvailable).
	UserCPUMs *int64 `json:"user_cpu_ms,omitempty"`
	SysCPUMs  *int64 `json:"sys_cpu_ms,omitempty"`
}

// Output is the row and part accounting: this invocation (fetched,
// filtered, emitted) versus the dataset (retained, durable, committed).
type Output struct {
	// Version classification is attempt-scoped, after filtering, and
	// present only for versions schemas (including zero counts).
	Versions      *int64 `json:"versions,omitempty"`
	DeleteMarkers *int64 `json:"delete_markers,omitempty"`
	Noncurrent    *int64 `json:"noncurrent,omitempty"`
	DistinctKeys  *int64 `json:"distinct_keys,omitempty"`
	LiveObjects   *int64 `json:"live_objects,omitempty"`
	RowsFetched   int64  `json:"rows_fetched"`   // listed from the source by this invocation, before the filters
	RowsFiltered  int64  `json:"rows_filtered"`  // fetched by this invocation and dropped by the filters (--include, ...)
	RowsEmitted   int64  `json:"rows_emitted"`   // fetched, kept by the filters and handed on in order (swath du, swath diag count: counted)
	RowsRetained  int64  `json:"rows_retained"`  // kept from the interrupted run (resume)
	RowsCommitted int64  `json:"rows_committed"` // in the committed dataset (0 unless committed)
	// RowsDurable is the rows in parts that stay in the output: the
	// committed dataset's, or after a failure the journaled parts' (kept
	// for swath resume; 0 without a journal).
	RowsDurable   int64 `json:"rows_durable"`
	PartsWritten  int   `json:"parts_written"` // written by this invocation (after a failure: journaled, durable)
	PartsRetained int   `json:"parts_retained"`
	// ObjectBytes sums the sizes of the objects the order stage accepted
	// from this invocation (the size column; absent for a schema without
	// one), every name once: a local file with several hard links counts
	// once per link, unlike output.totals. On a committed run exactly those
	// of rows_emitted, on a run that stopped also rows accepted and never
	// handed on.
	// OutputBytes is the dataset's part bytes (a text run's: output.text.bytes).
	ObjectBytes *int64 `json:"object_bytes,omitempty"`
	OutputBytes int64  `json:"output_bytes"`
	// CommittedObjectBytes is the committed dataset's object bytes, beside
	// rows_committed: every row's size summed as the writers wrote them
	// (retained parts and rows replayed from spill included, each row
	// once), the manifest's extensions.swath.committed_object_bytes. Absent unless
	// committed, and when unknown: a schema without a size column, or a
	// part journaled by a build before the count. 0 is a real total.
	CommittedObjectBytes *int64 `json:"committed_object_bytes,omitempty"`
	// Text is a text run's output (nil for a dataset or a dry run).
	Text *TextOutput `json:"text,omitempty"`
	// Totals is swath du's per-prefix totals: the depth, the prefixes and
	// their cap, the column summed and the total (nil without them).
	Totals *totals.Stats `json:"totals,omitempty"`
	// Remote is a remote dataset's account: its destination, uploads,
	// commit and orphan cleanup (nil for a local dataset).
	Remote *Remote `json:"remote,omitempty"`
}

// Committed accounts the dataset m, committed, as the output's dataset
// side: its rows (rows_committed, rows_durable), part bytes and object
// bytes. It is the account of an invocation that found the commit made
// (an earlier attempt's), which retained and emitted nothing itself.
func (o *Output) Committed(m *dataset.Manifest) {
	t := m.Totals()
	o.RowsCommitted, o.RowsDurable, o.OutputBytes, o.CommittedObjectBytes = t.Rows, t.Rows, t.Bytes, t.CommittedObjectBytes
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
}

// TextSink is how a text run's rows reached its output.
type TextSink struct {
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
	JobsAsked     = "encoders"          // --encoders (Limits.Encoders) set it
	JobsCores     = "cores"             // GOMAXPROCS: the encoder allowance carries more
	JobsAllowance = "encoder_allowance" // the encoder allowance carries fewer than GOMAXPROCS (admitTextJobs)
	JobsGzip      = "gzip"              // gzip: one stream
)

// Process is the process's account of itself.
type Process struct {
	// PeakRSSBytes is the process's peak resident set (Windows: peak
	// working set): absent (nil) where it is not measured
	// (Resources.RusageAvailable).
	PeakRSSBytes *int64 `json:"peak_rss_bytes,omitempty"`
}

// Resources the run used. Wait times sum over goroutines.
type Resources struct {
	// RusageAvailable says whether this platform measured the CPU times and
	// peak RSS; false leaves them out rather than reporting zeros.
	RusageAvailable   bool  `json:"rusage_available"`
	PeakBufferedBytes int64 `json:"peak_buffered_bytes"` // peak reorder storage (the budget: invocation.sort_buffer_bytes)
	// FreeDiskMinBytes is the least free space seen on the filesystem the
	// run writes to (the spill directory's, else the output's), sampled
	// every second (HistoryEvery); absent for a run that writes nothing.
	FreeDiskMinBytes *int64 `json:"free_disk_min_bytes,omitempty"`
	// PeakHeapLiveBytes is the peak live heap while listing (the bytes a
	// garbage collection marked reachable, sampled every 100 ms): beside
	// invocation.sort_buffer_bytes, the memory the budget does not see.
	PeakHeapLiveBytes int64 `json:"peak_heap_live_bytes"`
	// PeakAheadRows is the most rows listed and not yet handed on at once
	// (ahead of the publish frontier, in memory or spilled);
	// MaxFrontierStuckNs the longest interval in which the order stage
	// handed nothing on. Both 0 for a dry run, which does not order.
	PeakAheadRows      int64 `json:"peak_ahead_rows"`
	MaxFrontierStuckNs int64 `json:"max_frontier_stuck_ns"`
	FrontierWaitNs     int64 `json:"frontier_wait_ns"`
	BudgetWaitNs       int64 `json:"budget_wait_ns"`
	// The order stage's waits by site, summed over goroutines, each with
	// the most goroutines waiting there at once: emitters in publish on a
	// full budget and workers at admission (together budget_wait_ns), and
	// idle workers, nothing queued to list (read-ahead waits included).
	PublishWaitNs        int64 `json:"publish_wait_ns"`
	PeakPublishWaiters   int64 `json:"peak_publish_waiters"`
	AdmissionWaitNs      int64 `json:"admission_wait_ns"`
	PeakAdmissionWaiters int64 `json:"peak_admission_waiters"`
	IdleWaitNs           int64 `json:"idle_wait_ns"`
	PeakIdleWorkers      int64 `json:"peak_idle_workers"`
	// EncoderWaitNs is the time the order stage waited for an encoder (nil
	// without output).
	EncoderWaitNs *int64 `json:"encoder_wait_ns,omitempty"`
	Spill         *Spill `json:"spill,omitempty"` // nil: spill was off
	// Drain is the order stage's drain (nil for a dry run).
	Drain *Drain `json:"drain,omitempty"`
	// Window is the frontier window (nil: the run was not planned from a
	// counted prior; docs/internals/ordering.md, "The frontier window").
	Window *Window `json:"window,omitempty"`
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
	// PartRowCuts counts parts closed by --part-max-rows before
	// their size target (deterministic, unlike time cuts).
	PartRowCuts int64 `json:"part_row_cuts"`
}

// Window is the frontier window's account (order.WindowStats): the tiles
// admitted (forced: by the drainer past a refusal, having drained every
// other; grown: held rows past 4 times the prior's), the window's rows when
// last evaluated and the term that set them (bound_by: rows, set, spill_cap
// or budget), the workers' waits at the window by the refusal that held
// them (rows, spill, growth) with their time and peak, the drainer's wait
// for the plan to read ahead, and the peaks of the tiles held, the rows
// charged to them and the bytes the order stage held (memory and live
// spill) at an admission; under the spill gate, the mean rows charged at
// its refusals, the most live spill behind the output and the drained
// certificates it evicted; and the wakes of the workers held at it on the
// paths that engage only with durable spill, by path.
type Window struct {
	Tiles             int64            `json:"tiles"`
	Forced            int64            `json:"forced"`
	Grown             int64            `json:"grown"`
	Rows              int64            `json:"rows"`
	BoundBy           string           `json:"bound_by"`
	Waits             map[string]int64 `json:"waits"`
	WaitNs            int64            `json:"wait_ns"`
	PeakWaiters       int64            `json:"peak_waiters"`
	PlanWaitNs        int64            `json:"plan_wait_ns"`
	PeakTiles         int64            `json:"peak_tiles"`
	PeakCreditRows    int64            `json:"peak_credit_rows"`
	PeakRetainedBytes int64            `json:"peak_retained_bytes"`
	SpillRows         int64            `json:"spill_rows"`
	PeakBehindBytes   int64            `json:"peak_behind_bytes"`
	Evicted           int64            `json:"evicted"`
	Wakes             map[string]int64 `json:"wakes"`
}

// Drain is the order stage's drain after listing and the time it took.
// Durations sum over goroutines where several wait.
type Drain struct {
	// TailNs is from provider_finished_at to the drain's last row: the
	// part of the run in which only the drain was left (0: listing never
	// finished).
	TailNs int64 `json:"tail_ns"`
	// The workers' idle time after provider_finished_at (nothing queued to
	// list; read-ahead waits included), summed over workers.
	WorkerIdleAfterNs int64 `json:"worker_idle_after_provider_finished_ns"`
	// The drainer's time beside frontier_wait_ns: reading spilled frames
	// itself (decoding included, and counted again in ReplayDecodeNs),
	// waiting for a frame the read-ahead was reading, the order guard, and
	// handing batches to the sink (encoder_wait_ns included).
	ReplayReadNs   int64 `json:"replay_read_ns"`
	ReplayDecodeNs int64 `json:"replay_decode_ns"`
	AheadWaitNs    int64 `json:"ahead_wait_ns"`
	OrderCheckNs   int64 `json:"order_check_ns"`
	SinkWriteNs    int64 `json:"sink_write_ns"`
	// WriterBeforeListingDone is the Parquet writer's account at
	// provider_finished_at (nil without a dataset, or when listing never
	// finished).
	WriterBeforeListingDone *ListingWriter `json:"writer_before_provider_finished,omitempty"`
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
	// What the encoders were admitted against: the encoder allowance (the
	// default row budget's share of the memory found, whatever
	// --sort-buffer says), each encoder's price (its writer, and for a
	// dataset in a bucket its upload buffer), and what bound the count at
	// start and after listing (Bound*: memory, cpu, descriptors, minimum,
	// explicit).
	EncoderAllowanceBytes       int64  `json:"encoder_allowance_bytes,omitempty"`
	EncoderWriterBytes          int64  `json:"encoder_writer_bytes,omitempty"`
	EncoderUploadBytes          int64  `json:"encoder_upload_bytes,omitempty"`
	EncodersAtStartBoundBy      string `json:"encoders_at_start_bound_by,omitempty"`
	EncodersAfterListingBoundBy string `json:"encoders_after_listing_bound_by,omitempty"`
}

// ListingWriter is the Parquet writer's account up to provider_finished_at
// (parquetout.WriterStats): the parts published and their rows, and the
// encoders' time on the batches written, encoding and in the parts' files or
// uploads.
type ListingWriter struct {
	Parts    int64 `json:"parts"`
	Rows     int64 `json:"rows"`
	EncodeNs int64 `json:"encode_ns"`
	OutputNs int64 `json:"output_ns"`
}

// Why the encoders after listing are what they are (Drain.EncoderAdmission).
const (
	AdmissionListingDone = "provider_finished" // raised when the provider finished (admitAfterListing)
	AdmissionExplicit    = "explicit"          // --encoders was given: never raised
	AdmissionAtCap       = "at_cap"            // the start already admitted the most the host carries
	AdmissionNotReached  = "not_reached"       // listing never finished (a failed or stopped run)
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
	// not_planned, row_budget or listing_active.
	Reason   string `json:"reason"`
	Segments int    `json:"segments"`
	Workers  int    `json:"workers"`
	Rows     int64  `json:"rows"`
	MaxRows  int64  `json:"max_rows"` // the largest segment's
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
	// BudgetWaits are the times a worker waited for the row budget before
	// reading or gathering a unit, BudgetWaitNs their time; PeakHeldBytes
	// the most the segments held at once beside the tree (the encoders'
	// queues, the units the workers read or gathered).
	BudgetWaits   int64 `json:"budget_waits"`
	BudgetWaitNs  int64 `json:"budget_wait_ns"`
	PeakHeldBytes int64 `json:"peak_held_bytes"`
	// WorkersBy is what set workers: encoders, processors, descriptors,
	// row_budget or segments. PerWorkerBytes is the budget one worker
	// holds at most, FreeBytes the budget the segments could take when
	// planned (less what the encoders' queues held): the row budget holds
	// 1 + (free_bytes - per_worker_bytes - the first's queue) /
	// per_worker_bytes workers.
	WorkersBy      string `json:"workers_by,omitempty"`
	PerWorkerBytes int64  `json:"per_worker_bytes"`
	FreeBytes      int64  `json:"free_bytes"`
	// TreeBytes is the budget bytes the tree held when the segments were
	// planned, beside what they may take (two budgets in all, at most).
	TreeBytes int64 `json:"tree_bytes"`
	// DurableFilesRead is the durable spill files the segments read
	// frames of; DurableBoundaries the segment boundaries inside one of
	// their frames.
	DurableFilesRead  int `json:"durable_files_read"`
	DurableBoundaries int `json:"durable_boundaries"`
	// DeferredRows is the rows a planned boundary moved past because they
	// continued the group of the key before it (versions schemas).
	DeferredRows int64 `json:"deferred_rows"`
	// CertificatesRead is the reused certificates (a resume's durable
	// spill) the segments replayed, CertifiedRows their rows (in rows), and
	// CertificateScanNs the plan's time reading them before the cut (in
	// plan_ns). Durable_files_read counts their files too.
	CertificatesRead  int   `json:"certificates_read"`
	CertifiedRows     int64 `json:"certified_rows"`
	CertificateScanNs int64 `json:"certificate_scan_ns"`
}

// SourceStats is the provider's statistics under its name ("s3", "local").
type SourceStats struct {
	Name  string `json:"name"`
	Stats any    `json:"stats"`
}

// SourceReport is the source's own report (nil before the listing ends, or
// for a source without one). Its Stats are the provider's own type.
func (r *Report) SourceReport() *source.Report { return r.report }

func (r *Report) setSource(sr source.Report) {
	r.report = &sr
	r.Invocation.Metadata = sr.Fields
	r.Outcome.Unreadable = sr.Coverage.Unreadable
	r.Outcome.Complete, r.Outcome.StopReason = sr.Coverage.Complete, sr.Coverage.StopReason
	r.Source = &SourceStats{Name: sr.Name, Stats: sr.Stats}
}

// finish stamps the end time and the resources, and stops the history.
func (r *Report) finish() {
	if r.sampler != nil {
		r.History, r.Resources.FreeDiskMinBytes = r.sampler.finish()
	}
	now := time.Now()
	r.Timings.FinishedAt = jsontime.Of(now)
	r.Timings.WallMs = now.Sub(r.Timings.StartedAt.Time).Milliseconds()
	r.setRusage(rusage())
	if r.Attempt != nil {
		r.Timings.SessionMs = now.Sub(r.Attempt.StartedAt.Time).Milliseconds()
	}
	r.requests()
}

// setRusage records the process's measurements, or that there are none.
func (r *Report) setRusage(user, sys time.Duration, peak int64, ok bool) {
	r.Resources.RusageAvailable = ok
	if !ok {
		r.Timings.UserCPUMs, r.Timings.SysCPUMs, r.Process.PeakRSSBytes = nil, nil, nil
		return
	}
	u, s := user.Milliseconds(), sys.Milliseconds()
	r.Timings.UserCPUMs, r.Timings.SysCPUMs, r.Process.PeakRSSBytes = &u, &s, &peak
}

// requests fills Requests, Cost and Efficiency from the source's request
// account.
func (r *Report) requests() {
	rq, ok := r.src.(source.Requested)
	if !ok {
		return
	}
	r.live = rq.Requests()
	n := r.live.Attempts
	r.Requests = &Requests{HTTPAttempts: n, Retries: r.live.Retries, Throttles: r.live.Throttles, Timeouts: r.live.Timeouts}
	c := &Cost{}
	if p, ok := r.src.(source.Priced); ok {
		if rate, basis := p.Tariff(); basis != "" {
			usd := float64(n) / 1000 * rate
			c.CostUSD, c.Basis = &usd, &CostBasis{RatePer1kUSD: rate, Source: basis}
		}
	}
	r.Cost = c
	e := &Efficiency{}
	if ms := r.Timings.ListingMs; ms > 0 {
		e.KeysPerSec = float64(r.Output.RowsEmitted) * 1000 / float64(ms)
	}
	if rows := r.Output.RowsFetched; rows > 0 {
		v := float64(n) * 1000 / float64(rows)
		e.HTTPAttemptsPer1kRows = &v
	}
	r.Efficiency = e
}

// BuildInfo is this binary's name, module version and VCS revision.
func BuildInfo() Build { return buildinfo.Read() }
