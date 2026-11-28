// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/internal/command"
	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/source"
)

// ListOptions is one run of swath list. Source is the SOURCE the command
// takes: a local directory or file, or an s3://, gs:// or az:// address.
// Every other field but Stdout, Stderr and Progress is the swath list flag
// its flag tag names, with the meaning and the accepted values swath list
// --help gives it. A zero field leaves its flag out, so the flag's default
// applies; a pointer field, for a flag whose zero is not the same as its
// absence, is left out when nil. A field whose tag names two flags gives
// the first when true and the second when false.
type ListOptions struct {
	Source string

	// Stdout takes what swath list writes to stdout: the rows without
	// Output, and the report when Report is "-". Stderr takes its log, its
	// progress line, its statistics and its outcome. Nil discards it. A
	// writer that is not an *os.File is not a terminal. Any writer will
	// do, one whose type cannot be compared (a func type) included.
	Stdout, Stderr io.Writer

	// Progress, when set, is handed the run's progress at the progress
	// file's cadence (every 2 seconds, or a quarter of StallTimeout when
	// that is shorter), from one goroutine at a time; a call that is slow
	// misses the snapshots taken meanwhile and gets the latest. A run that
	// started hands it its final progress last, once the run has ended and
	// its dataset, report and outcome are written, before List returns;
	// only that progress says done or failed. List waits for every call
	// to return. A call that calls List waits for the run that calls it to
	// end, as any other call does.
	Progress func(ListProgress)

	// Output.
	Output                   string   `flag:"output"`
	Format                   string   `flag:"format"`
	NoHeader                 bool     `flag:"no-header"`
	Columns                  []string `flag:"columns"`
	Overwrite                bool     `flag:"overwrite"`
	WorkDir                  string   `flag:"work-dir"`
	OutputGCSCredentialsFile string   `flag:"output-gcs-credentials-file"`
	OutputS3Region           string   `flag:"output-s3-region"`
	OutputS3Profile          string   `flag:"output-s3-profile"`
	OutputAzureSASToken      string   `flag:"output-azure-sas-token"`
	OutputEndpointURL        string   `flag:"output-endpoint-url"`

	// Filters. A size is in bytes.
	Include       []string  `flag:"include"`
	Exclude       []string  `flag:"exclude"`
	IncludeRegex  []string  `flag:"include-regex"`
	ExcludeRegex  []string  `flag:"exclude-regex"`
	MinSize       *int64    `flag:"min-size"`
	MaxSize       *int64    `flag:"max-size"`
	ModifiedSince time.Time `flag:"modified-since"`
	ModifiedUntil time.Time `flag:"modified-until"`
	StorageClass  []string  `flag:"storage-class"`

	// Performance. A size is in bytes.
	Workers      int            `flag:"workers"`
	Encoders     int            `flag:"encoders"`
	SortBuffer   int64          `flag:"sort-buffer"`
	SpillBudget  int64          `flag:"spill-budget"`
	PartSize     int64          `flag:"part-size"`
	PartInterval *time.Duration `flag:"part-interval"`
	PartMaxRows  int64          `flag:"part-max-rows"`
	SeedFrom     string         `flag:"seed-from"`

	// Limits.
	MaxDuration      time.Duration  `flag:"max-duration"`
	StallTimeout     *time.Duration `flag:"stall-timeout"`
	SpillWaitTimeout time.Duration  `flag:"spill-wait-timeout"`
	RequestTimeout   *time.Duration `flag:"request-timeout"`
	RequestRate      float64        `flag:"request-rate"`
	NoAdaptiveLimit  bool           `flag:"no-adaptive-limit"`

	// Reporting. Verbose and Quiet give their flag that many times.
	Report           string        `flag:"report"`
	Verbose          int           `flag:"verbose"`
	Quiet            int           `flag:"quiet"`
	ProgressLine     *bool         `flag:"progress,no-progress"`
	ProgressInterval time.Duration `flag:"progress-interval"`
	Stats            *bool         `flag:"stats,no-stats"`
	Color            string        `flag:"color"`
	LogFormat        string        `flag:"log-format"`

	// Object stores.
	Anonymous   bool   `flag:"anonymous"`
	EndpointURL string `flag:"endpoint-url"`
	CABundle    string `flag:"ca-bundle"`
	NoVerifySSL bool   `flag:"no-verify-ssl"`
	AllVersions bool   `flag:"all-versions"`

	// A local directory.
	Metadata      string `flag:"metadata"`
	NoDirs        bool   `flag:"no-dirs"`
	NoLinkTarget  bool   `flag:"no-link-target"`
	OneFileSystem bool   `flag:"one-file-system"`

	// Azure Blob Storage.
	AzureSASToken          string `flag:"azure-sas-token"`
	AzureFetchMetadata     bool   `flag:"azure-fetch-metadata"`
	AzureFetchTags         bool   `flag:"azure-fetch-tags"`
	AzureFetchImmutability bool   `flag:"azure-fetch-immutability"`
	AzureAssumeFlat        bool   `flag:"azure-assume-flat"`

	// Google Cloud Storage.
	GCSCredentialsFile string `flag:"gcs-credentials-file"`
	GCSBillingProject  string `flag:"gcs-billing-project"`

	// S3.
	S3Region                     string        `flag:"s3-region"`
	S3Profile                    string        `flag:"s3-profile"`
	S3RequesterPays              bool          `flag:"s3-requester-pays"`
	S3PathStyle                  *bool         `flag:"s3-path-style,no-s3-path-style"`
	S3BearerTokenCommand         string        `flag:"s3-bearer-token-command"`
	S3BearerTokenRefreshInterval time.Duration `flag:"s3-bearer-token-refresh-interval"`
	S3FIPS                       *bool         `flag:"s3-fips"`
	S3DualStack                  *bool         `flag:"s3-dualstack"`
	S3FetchOwner                 bool          `flag:"s3-fetch-owner"`
	S3Compat                     string        `flag:"s3-compat"` // auto (""), s3 or b2: which store's differences to allow for
}

// ListProgress is a run's progress, as its progress file
// (DIR/.swath/progress.json) gives it under the names in parentheses.
type ListProgress struct {
	AttemptID    string    // the run report's attempt.id (attempt_id)
	RunID        string    // the run report's run.id, once the output is opened (run_id)
	State        string    // the run's phase (state)
	RowsFetched  int64     // rows listed so far, directories and versions included (rows_fetched)
	RowsRetained int64     // rows a resume kept from the run it continues (rows_retained)
	RowsEncoded  int64     // rows written into parts (rows_encoded)
	PartsWritten int64     // parts published (parts_written)
	ObjectBytes  *int64    // the sizes of the objects listed so far, summed; nil while unknown (object_bytes)
	HTTPAttempts int64     // requests sent, retries included (http_attempts)
	Retries      int64     // requests repeated (retries)
	Throttles    int64     // throttle answers from the service (throttles)
	StartedAt    time.Time // zero before the run starts (started_at)
	AsOf         time.Time // when the snapshot was taken (as_of)
}

// ListStatus is how a run of List ended.
type ListStatus int

const (
	// ListCompleted is a run that ended with exit 0: its output is
	// written, or there was nothing to do.
	ListCompleted ListStatus = iota + 1
	// ListCanceled is a run that ended early because its context ended:
	// the run stopped on it (its error is the context's error or cause),
	// or the context ended before the call got its turn. A context that
	// ends after the run failed otherwise, while the run's output is
	// finalized or Progress runs, leaves the result as it is.
	ListCanceled
	// ListFailed is any other run that did not complete, a partial output
	// (exit 3) included.
	ListFailed
)

func (s ListStatus) String() string {
	switch s {
	case ListCompleted:
		return "completed"
	case ListCanceled:
		return "canceled"
	case ListFailed:
		return "failed"
	}
	return "ListStatus(" + strconv.Itoa(int(s)) + ")"
}

// ListResult is how a run of List ended. Class, Cause, ExitCode and
// Resumable are the outcome swath list records for the same end, in the run
// report and in DIR/.swath/error.json (the exit codes and classes of
// docs/reference/exit-codes.md); ExitCode is what swath list exits with.
type ListResult struct {
	Status    ListStatus
	Class     string // ok, usage, refused, fatal, incomplete, ...
	Cause     string // the provider's class of the error, when one is known
	ExitCode  int
	Resumable bool // swath resume DIR can continue the run
	Complete  bool // the output covers its whole scope
}

// The reasons a run failed that List's error matches with errors.Is. A
// canceled run's error matches its context's error (context.Canceled or
// context.DeadlineExceeded) instead.
var (
	// ErrOutputInUse: another run, in this process or another, holds the
	// output's lock. That run is left as it is.
	ErrOutputInUse = errors.New("the output is in use by another run")
	// ErrSourceNotFound: the bucket, the container or the local root does
	// not exist.
	ErrSourceNotFound = errors.New("the source does not exist")
	// ErrSourceDenied: the source cannot be read for want of access: the
	// service refused the credentials or the access, or there were none,
	// or the local root cannot be opened.
	ErrSourceDenied = errors.New("the source cannot be read: access denied")
	// ErrUsage: the options were refused before the run started (class
	// usage, exit 2), with the reason the command line gives.
	ErrUsage = errors.New("the options are refused")
)

// ListError is the error of a run that did not complete. Its message is
// the one swath list prints; it matches the reasons above with errors.Is
// and wraps the run's own error, whose text may change in any release.
type ListError struct {
	Result ListResult
	err    error // the run's
	reason error // one of the reasons above, or nil
	ctx    error // the context's error, for a canceled run
}

func (e *ListError) Error() string { return e.err.Error() }

// Unwrap returns the run's error, its reason and, for a canceled run, its
// context's error.
func (e *ListError) Unwrap() []error {
	errs := []error{e.err}
	for _, r := range []error{e.reason, e.ctx} {
		if r != nil {
			errs = append(errs, r)
		}
	}
	return errs
}

// List runs swath list in this process, from opts instead of arguments,
// through the code Main runs: the same output, files, run report and
// outcome. It returns the result and, when the run did not complete, a
// *ListError.
//
// Unlike Main, List sets no signal handling and no forced exit: the run
// stops when ctx ends, and List returns once it has stopped and recorded
// its outcome. Like Main it turns the Azure SDK's own log off. Calls to
// List in one process run one at a time: a call waits for the run before
// it to end, or for its own ctx to end, when it returns canceled and runs
// nothing, as a call whose ctx had ended does. A panic out of List (from
// Stdout or Stderr) leaves the next call its turn. Stderr is written through a goroutine; a line still waiting
// for a Stderr that does not take it can be written after List returns.
func List(ctx context.Context, opts ListOptions) (ListResult, error) {
	stdout, stderr := opts.Stdout, opts.Stderr
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	var progress func(inventory.ProgressSnapshot)
	if opts.Progress != nil {
		progress = func(s inventory.ProgressSnapshot) { opts.Progress(progressOf(s)) }
	}
	end := command.List(ctx, listArgs(&opts), stdout, stderr, progress)
	r := ListResult{Status: ListCompleted, Class: end.Class, Cause: end.Cause, ExitCode: end.ExitCode, Resumable: end.Resumable, Complete: end.Complete}
	if end.Err == nil {
		return r, nil
	}
	e := &ListError{err: end.Err}
	switch end.Reason {
	case command.ReasonOutputInUse:
		e.reason = ErrOutputInUse
	case command.ReasonSourceNotFound:
		e.reason = ErrSourceNotFound
	case command.ReasonSourceDenied:
		e.reason = ErrSourceDenied
	}
	if end.Class == inventory.ClassUsage {
		e.reason = ErrUsage
	}
	r.Status = ListFailed
	if end.Canceled != nil {
		r.Status, e.ctx = ListCanceled, end.Canceled
	}
	e.Result = r
	return r, e
}

// listArgs is opts as swath list's arguments: each flag given as
// --name=value, which takes any value as it is, then -- and the source,
// which takes any operand as it is; an empty source is none.
func listArgs(opts *ListOptions) []string {
	var args []string
	v := reflect.ValueOf(opts).Elem()
	for i := range v.NumField() {
		tag := v.Type().Field(i).Tag.Get("flag")
		if tag == "" {
			continue
		}
		on, off, _ := strings.Cut(tag, ",")
		with := func(value string) { args = append(args, "--"+on+"="+value) }
		switch x := v.Field(i).Interface().(type) {
		case string:
			if x != "" {
				with(x)
			}
		case bool:
			if x {
				args = append(args, "--"+on)
			}
		case int:
			switch on {
			case "verbose", "quiet":
				for range x {
					args = append(args, "--"+on)
				}
			default:
				if x != 0 {
					with(strconv.Itoa(x))
				}
			}
		case int64:
			if x != 0 {
				with(strconv.FormatInt(x, 10))
			}
		case *int64:
			if x != nil {
				with(strconv.FormatInt(*x, 10))
			}
		case float64:
			if x != 0 {
				with(strconv.FormatFloat(x, 'g', -1, 64))
			}
		case time.Duration:
			if x != 0 {
				with(source.FormatDuration(x))
			}
		case *time.Duration:
			if x != nil {
				with(source.FormatDuration(*x))
			}
		case time.Time:
			if !x.IsZero() {
				with(x.Format(time.RFC3339Nano))
			}
		case []string:
			for _, s := range x {
				with(s)
			}
		case *bool:
			switch {
			case x == nil:
			case off == "":
				with(strconv.FormatBool(*x))
			case *x:
				args = append(args, "--"+on)
			default:
				args = append(args, "--"+off)
			}
		default:
			panic(fmt.Sprintf("ListOptions.%s: no rendering for %T", v.Type().Field(i).Name, x))
		}
	}
	if opts.Source == "" {
		return args // no SOURCE: refused as swath list refuses it
	}
	return append(args, "--", opts.Source)
}

// progressOf is s as a ListProgress.
func progressOf(s inventory.ProgressSnapshot) ListProgress {
	return ListProgress{AttemptID: s.AttemptID, RunID: s.RunID, State: s.State, RowsFetched: s.Rows, RowsRetained: s.Retained, RowsEncoded: s.Encoded,
		PartsWritten: s.Parts, ObjectBytes: s.ObjectBytes, HTTPAttempts: s.Requests, Retries: s.Retries, Throttles: s.Throttles,
		StartedAt: s.StartedAt.Time, AsOf: s.AsOf.Time}
}
