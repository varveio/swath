// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
)

// The reasons a list run embedded in another program ended as it did
// (ListEnd.Reason), besides its class: what the public command package
// turns into its sentinel errors.
const (
	ReasonOutputInUse    = "output_in_use"    // another process holds the output's lock
	ReasonSourceNotFound = "source_not_found" // the bucket, container or local root does not exist
	ReasonSourceDenied   = "source_denied"    // the source refused the credentials or the access, or had none
)

// ListEnd is how a list run embedded in another program ended (List): the
// outcome Main exits with, as error.json and the run report record it.
type ListEnd struct {
	Err       error  // the run's error, nil when it completed
	Class     string // inventory.Class*
	Cause     string
	ExitCode  int
	Resumable bool
	Complete  bool   // the output covers its whole scope
	Reason    string // a Reason*, "" for none of them
	// Canceled is the caller's context's error when the run's error, as
	// it was settled (inventory.Run returned, or the command stopped
	// before it), is that context's ending (canceledBy), nil otherwise:
	// the run ended because it was canceled. A context that ends after the
	// run failed otherwise, or while it is finalized or a progress
	// callback runs, does not make it so.
	Canceled error
}

// embedding is what a list run embedded in another program adds to the
// command's (command.embed): a progress callback, the caller's context,
// and what the run tells it back.
type embedding struct {
	ctx      context.Context // the caller's
	progress func(inventory.ProgressSnapshot)
	feed     chan inventory.ProgressSnapshot
	fed      chan struct{}
	run      *embedRun // nil: the run stopped before it listed (inventory.Run)
	final    *inventory.ProgressSnapshot
	canceled error // canceledBy(ctx, the run's error), once it is settled
}

// embedRun is what a run that listed tells a Reason by.
type embedRun struct {
	local  bool // the source is a local path
	opened bool // the source opened (the report names its schema)
}

// start starts the goroutine that hands snapshots to the callback, so that
// a slow callback delays neither the progress file nor the stall
// watchdog (supervise), which offer it the latest snapshot.
func (e *embedding) start() {
	if e == nil || e.progress == nil {
		return
	}
	e.feed, e.fed = make(chan inventory.ProgressSnapshot, 1), make(chan struct{})
	go func() {
		defer close(e.fed)
		for s := range e.feed {
			e.progress(s)
		}
	}()
}

// offer queues s for the callback in place of a snapshot it has not
// taken yet; it never waits. Only supervise's goroutine offers. A done or
// failed snapshot is not offered: the callback has the run's end once the
// command has ended (deliver), not before its report is written.
func (e *embedding) offer(s inventory.ProgressSnapshot) {
	if e == nil || e.feed == nil || s.State == inventory.StateDone || s.State == inventory.StateFailed {
		return
	}
	select {
	case e.feed <- s:
		return
	default:
	}
	select {
	case <-e.feed: // the older snapshot, not taken yet
	default:
	}
	select {
	case e.feed <- s:
	default:
	}
}

// stopped records, once inventory.Run returned err and supervise
// stopped, r, whether err is the caller's context's ending, and the run's
// final snapshot when it started, as the progress file keeps it; it ends
// the feed without waiting for the callback, which may be waiting for what
// the command writes after the run (deliver).
func (e *embedding) stopped(pr *inventory.Progress, r embedRun, err error) {
	if e == nil {
		return
	}
	e.run, e.canceled = &r, canceledBy(e.ctx, err)
	if e.feed == nil {
		return
	}
	close(e.feed)
	if pr.Started() {
		s := pr.Snapshot()
		e.final = &s
	}
}

// deliver waits for the callback's goroutine and hands the callback the
// run's final snapshot, once the command has ended: its report, dataset
// and outcome are written.
func (e *embedding) deliver() {
	if e.feed == nil {
		return
	}
	<-e.fed
	if e.final != nil {
		e.progress(*e.final)
	}
}

// listing admits one List at a time: the command keeps its stderr, its
// terminal detection and its event log in package state. A call waits for
// it only as long as its context lets it.
var listing = make(chan struct{}, 1)

// List runs swath list args (its flags, then -- and SOURCE) in this
// process for a program that embeds swath, through the code Main runs, and
// returns how it ended; the lines Main prints at the end go to stderr too.
// Unlike Main it sets no signal handling and no forced exit: cancelling ctx
// stops the run. It turns the Azure SDK's own log off, as Main does. Calls
// run one at a time; a call whose ctx ends while it waits for its turn, or
// has ended when it gets it, returns ctx's error, canceled, and runs
// nothing. progress (nil: none) is
// handed a snapshot at the progress file's cadence, the latest one when it
// is slow, and the final one once the command has ended, before List
// returns; List waits for it to return.
func List(ctx context.Context, args []string, stdout, stderr io.Writer, progress func(inventory.ProgressSnapshot)) ListEnd {
	notRun := func(err error) ListEnd {
		o := classify(err, nil, "")
		return ListEnd{Err: err, Class: o.Class, ExitCode: o.ExitCode, Canceled: ctx.Err()}
	}
	select {
	case listing <- struct{}{}:
	case <-ctx.Done():
		return notRun(fmt.Errorf("waiting for another run in this process: %w", ctx.Err()))
	}
	if ctx.Err() != nil { // the select takes the turn as readily as the end
		<-listing
		return notRun(fmt.Errorf("before the run: %w", ctx.Err()))
	}
	e := &embedding{ctx: ctx, progress: progress}
	end := func() ListEnd {
		defer func() { <-listing }() // a panic out of the command too
		return list(ctx, e, args, stdout, stderr)
	}()
	e.deliver()
	return end
}

// list is List's turn: the command, run with the package's writers set to
// stdout and stderr, and its end.
func list(ctx context.Context, e *embedding, args []string, stdout, stderr io.Writer) ListEnd {
	azlog.SetListener(nil) // as Main: it quotes request URLs, a SAS included
	prevErr, prevOut, prevErrFile := errOut, stdoutFile, stderrFile
	errOut = keyable(stderr)
	stdoutFile, stderrFile = streamFile(stdout), streamFile(stderr)
	defer func() { errOut, stdoutFile, stderrFile = prevErr, prevOut, prevErrFile }()
	err := runWith(ctx, command{embed: e}, args, stdout, nil, false, nil)
	if e.run == nil {
		e.canceled = canceledBy(ctx, err) // the command stopped before the run: its error is settled now
	}
	o := concluded(err)
	end := ListEnd{Class: o.Class, Cause: o.Cause, ExitCode: o.ExitCode, Resumable: o.Resumable, Complete: o.Complete}
	if o.ExitCode != exitOK {
		end.Err, end.Canceled = err, e.canceled
		if errors.Is(err, pflag.ErrHelp) {
			end.Err = usageError{errors.New("help is not a run")}
		}
		if e.run != nil {
			end.Reason = e.run.reason(err)
		}
	}
	return end
}

// canceledBy is ctx's error when err is ctx's ending: it matches ctx's
// cause (a run stopped by it fails with that) or ctx's error. nil
// otherwise, also when ctx ended after err was settled by something else.
func canceledBy(ctx context.Context, err error) error {
	if c := ctx.Err(); c != nil && err != nil && (errors.Is(err, context.Cause(ctx)) || errors.Is(err, c)) {
		return c
	}
	return nil
}

// writerRef is a writer that can key a map (diagSinks) whatever it holds.
type writerRef struct{ io.Writer }

// keyable is w, or w behind a pointer when w's value cannot be compared
// (a func or map type with a Write method), which a map key must be.
func keyable(w io.Writer) io.Writer {
	if w != nil && !reflect.ValueOf(w).Comparable() {
		return &writerRef{w}
	}
	return w
}

// reason is the Reason of a run that listed and failed with err. A
// provider's class is the source's when the error carries it as objhttp's
// (a Failure or a CredentialError): the source's clients are the only ones
// that make them, the destination's (objfile) never do. A local error is
// the source's only when the local source did not open, which is the run's
// one use of the file system before its output.
func (r embedRun) reason(err error) string {
	var f *objhttp.Failure
	var ce *objhttp.CredentialError
	cause := ""
	if errors.As(err, &f) || errors.As(err, &ce) {
		cause = string(objhttp.Classify(err))
	}
	switch {
	case errors.Is(err, journal.ErrLocked):
		return ReasonOutputInUse
	case cause == source.CauseNotFound, r.local && !r.opened && errors.Is(err, fs.ErrNotExist):
		return ReasonSourceNotFound
	case cause == string(objhttp.ClassAuth), cause == string(objhttp.ClassAccessDenied), r.local && !r.opened && errors.Is(err, fs.ErrPermission):
		return ReasonSourceDenied
	}
	return ""
}

// ListFlags are the flags swath list takes, as its help lists them.
func ListFlags() []string {
	var names []string
	newFlags(&options{}, false).VisitAll(func(fl *pflag.Flag) {
		if shown(fl, command{}) {
			names = append(names, fl.Name)
		}
	})
	return names
}
