// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"slices"
	"sync"

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
}

// embedding is what a list run embedded in another program adds to the
// command's (command.embed): a progress callback, and what the run tells
// it back.
type embedding struct {
	progress func(inventory.ProgressSnapshot)
	feed     chan inventory.ProgressSnapshot
	fed      chan struct{}
	run      *embedRun // nil: the run stopped before it listed (inventory.Run)
}

// embedRun is what a run that listed tells a Reason by.
type embedRun struct {
	local  bool // the source is a local path
	remote bool // the output is in a bucket
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
// taken yet; it never waits. Only supervise's goroutine offers.
func (e *embedding) offer(s inventory.ProgressSnapshot) {
	if e == nil || e.feed == nil {
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

// finish records r, and waits for the callback's goroutine, after
// supervise stopped, and hands the callback the run's final snapshot when
// the run started, as the progress file keeps it.
func (e *embedding) finish(pr *inventory.Progress, r embedRun) {
	if e == nil {
		return
	}
	e.run = &r
	if e.feed == nil {
		return
	}
	close(e.feed)
	<-e.fed
	if pr.Started() {
		e.progress(pr.Snapshot())
	}
}

// listing serializes List: the command keeps its stderr, its terminal
// detection and its event log in package state.
var listing sync.Mutex

// List runs swath list args (its flags, then -- and SOURCE) in this
// process for a program that embeds swath, through the code Main runs, and
// returns how it ended; the lines Main prints at the end go to stderr too.
// Unlike Main it sets no signal handling and no forced exit: cancelling ctx
// stops the run. It turns the Azure SDK's own log off, as Main does. Calls
// run one at a time. progress (nil: none) is handed a snapshot at the
// progress file's cadence, the latest one when it is slow, and the final
// one before List returns.
func List(ctx context.Context, args []string, stdout, stderr io.Writer, progress func(inventory.ProgressSnapshot)) ListEnd {
	listing.Lock()
	defer listing.Unlock()
	azlog.SetListener(nil) // as Main: it quotes request URLs, a SAS included
	prevErr, prevOut, prevErrFile := errOut, stdoutFile, stderrFile
	errOut = stderr
	stdoutFile, stderrFile = streamFile(stdout), streamFile(stderr)
	defer func() { errOut, stdoutFile, stderrFile = prevErr, prevOut, prevErrFile }()
	e := &embedding{progress: progress}
	err := runWith(ctx, command{embed: e}, args, stdout, nil, false, nil)
	o := concluded(err)
	end := ListEnd{Class: o.Class, Cause: o.Cause, ExitCode: o.ExitCode, Resumable: o.Resumable, Complete: o.Complete}
	if o.ExitCode != exitOK {
		end.Err = err
		if errors.Is(err, pflag.ErrHelp) {
			end.Err = usageError{errors.New("help is not a run")}
		}
		if e.run != nil {
			end.Reason = e.run.reason(err, o.Cause)
		}
	}
	return end
}

// reason is the Reason of a run that listed and failed with err, cause its
// provider's class. A provider's class is the source's only when the
// output is not in a bucket, whose errors the same provider classifies; a
// local error is the source's only when the local source did not open,
// which is the run's one use of the file system before its output.
func (r embedRun) reason(err error, cause string) string {
	switch {
	case errors.Is(err, journal.ErrLocked):
		return ReasonOutputInUse
	case !r.remote && cause == source.CauseNotFound, r.local && !r.opened && errors.Is(err, fs.ErrNotExist):
		return ReasonSourceNotFound
	case !r.remote && slices.Contains([]string{string(objhttp.ClassAuth), string(objhttp.ClassAccessDenied)}, cause), r.local && !r.opened && errors.Is(err, fs.ErrPermission):
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
