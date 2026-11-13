// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/varveio/swath/internal/listing"
)

// The profiles' phase files beside FILE: the listing phase (from the start
// to the end of listing), the tail after it (CPU) and the process's end
// (memory). A side-path check covers each (checkSidePaths).
const (
	phaseListing = ".listing"
	phaseTail    = ".tail"
	phaseExit    = ".exit"
)

// The run report's kinds of profile file (listing.ProfileFile), in the
// order it lists them.
const (
	kindCPUListing = "cpu_listing"
	kindCPUTail    = "cpu_tail"
	kindMemListing = "mem_listing"
	kindMemExit    = "mem_exit"
)

// A profile file's status in the run report (listing.ProfileFile).
const (
	statusPlanned = "planned" // to be written later in the run
	statusWritten = "written"
	statusFailed  = "failed"
)

// defaultMemProfileRate is runtime.MemProfileRate while SWATH_MEMPROFILE is
// set and SWATH_MEMPROFILERATE is not.
const defaultMemProfileRate = 64 << 10

// memProfileRate parses SWATH_MEMPROFILERATE (bytes allocated per sample;
// 0 turns memory profiling off, 1 records every allocation); "" is the
// default.
func memProfileRate(v string) (int, error) {
	if v == "" {
		return defaultMemProfileRate, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("SWATH_MEMPROFILERATE %q: want a byte count, 0 or more", v)
	}
	return n, nil
}

// profiler writes the development profiles SWATH_CPUPROFILE and
// SWATH_MEMPROFILE name, split at the end of listing (listingDone, the
// event timings.listing_done_at records). The CPU profile runs from the
// start into FILE and FILE.listing (the same bytes: FILE is the listing
// phase, no longer the whole run, since Go runs one CPU profile at a time
// and keeps no whole-run profile beside the phases') until the phase switch
// stops it, a little after the boundary (cutDelay), and from the switch to
// the end into FILE.tail; go tool pprof FILE.listing FILE.tail merges them.
// The allocation profile, cumulative, is written at the switch into
// FILE.listing and at the end into FILE.exit and FILE (the whole run, as
// before the split); go tool pprof -base FILE.listing FILE.exit is the
// tail's. Without a boundary (listing never finished, a dry run) the
// listing-phase CPU files hold the whole run and no tail or memory listing
// file is written. A nil profiler (neither variable set) does nothing.
//
// Two locks: io serializes the profiles' writes (the switch, stop), which
// may block on a slow file; mu guards the state account reads, and is
// never held across a write, so the run report's account never waits for
// one.
type profiler struct {
	cpu, mem string // the resolved paths (checkSidePaths); "" off
	rate     int    // runtime.MemProfileRate with mem
	warn     func(string)

	io     sync.Mutex
	cpuOut []*profFile // the running CPU profile's files (under io); nil: none running

	mu       sync.Mutex
	files    map[string][]*profFile // by kind
	boundary time.Time              // when listing finished (zero: not yet, or never)
	switched chan struct{}          // closed once the switch listingDone started has run
	cut      bool                   // the switch stopped the listing CPU profile
	cutDelay time.Duration          // from the boundary to that stop
	gap      time.Duration          // from that stop to the tail's start
	tail     bool                   // the tail's CPU profile started
	stopped  bool                   // stop began: no phase starts after it
}

// profFile is one profile file: where it goes, what it holds and how its
// write went (under the profiler's mu). w is its writer while it is being
// written (under io), recording the first write error.
type profFile struct {
	kind, path, status, err string
	w                       *errWriter
}

// errWriter writes to f and keeps the first write error (a short write's
// included): runtime/pprof's
// CPU profile writer reports none.
type errWriter struct {
	f   *os.File
	err error
}

func (w *errWriter) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.f.Write(b)
	if err == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

// fanout writes each Write to every one of its files independently: one
// that fails (its errWriter keeps the error and takes no more) does not
// stop the others, as io.MultiWriter's first failure would. It fails only
// once every file has.
type fanout []*profFile

func (o fanout) Write(b []byte) (int, error) {
	var err error
	ok := false
	for _, f := range o {
		if _, e := f.w.Write(b); e == nil {
			ok = true
		} else {
			err = e
		}
	}
	if !ok {
		return 0, err
	}
	return len(b), nil
}

// startProfiles starts the profiles cpu and mem name ("" off; resolved by
// checkSidePaths), with runtime.MemProfileRate rate while mem is on. It
// returns nil when both are off.
func startProfiles(cpu, mem string, rate int, warn func(string)) (*profiler, error) {
	if cpu == "" && mem == "" {
		return nil, nil
	}
	p := &profiler{cpu: cpu, mem: mem, rate: rate, warn: warn, files: map[string][]*profFile{}}
	if mem != "" {
		runtime.MemProfileRate = rate
		p.plan(kindMemExit, mem+phaseExit, mem)
	}
	if cpu != "" {
		if err := p.startCPU(p.plan(kindCPUListing, cpu, cpu+phaseListing)); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// plan adds the files paths name, of kind, as planned.
func (p *profiler) plan(kind string, paths ...string) []*profFile {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.planLocked(kind, paths...)
}

// planLocked is plan under mu.
func (p *profiler) planLocked(kind string, paths ...string) []*profFile {
	fs := make([]*profFile, len(paths))
	for i, path := range paths {
		fs[i] = &profFile{kind: kind, path: path, status: statusPlanned}
		p.files[kind] = append(p.files[kind], fs[i])
	}
	return fs
}

// settle records how the writes of fs went: each file's own write or
// close error, else err (a failure of them all: an open, the profile's
// encoding). Only a file with neither is written. A failure is warned of
// after mu is released: stderr may block.
func (p *profiler) settle(fs []*profFile, err error) {
	var failed []string
	p.mu.Lock()
	for _, f := range fs {
		e := err
		if f.w != nil && f.w.err != nil {
			e = f.w.err
		}
		if e != nil {
			f.status, f.err = statusFailed, e.Error()
			failed = append(failed, fmt.Sprintf("profile %s: %v", f.path, e))
		} else {
			f.status = statusWritten
		}
		f.w = nil
	}
	p.mu.Unlock()
	for _, m := range failed {
		if p.warn != nil {
			p.warn(m)
		}
	}
}

// open creates fs's files; it closes those it opened and returns the first
// error if one fails.
func open(fs []*profFile) (io.Writer, error) {
	for i, f := range fs {
		file, err := createNoFollow(f.path)
		if err != nil {
			closeFiles(fs[:i])
			return nil, err
		}
		f.w = &errWriter{f: file}
	}
	return fanout(fs), nil
}

// closeFiles closes fs's open files, recording a close error as a write
// error.
func closeFiles(fs []*profFile) {
	for _, f := range fs {
		if f.w != nil {
			if err := f.w.f.Close(); f.w.err == nil {
				f.w.err = err
			}
		}
	}
}

// startCPU starts the CPU profile into fs (under io, or before the
// profiler is shared). On failure fs are settled as failed.
func (p *profiler) startCPU(fs []*profFile) error {
	w, err := open(fs)
	if err == nil {
		if err = pprof.StartCPUProfile(w); err != nil {
			closeFiles(fs)
		}
	}
	if err != nil {
		p.settle(fs, err)
		return err
	}
	p.cpuOut = fs
	return nil
}

// stopCPU stops the running CPU profile, if any, and settles its files
// (under io).
func (p *profiler) stopCPU() {
	if p.cpuOut == nil {
		return
	}
	pprof.StopCPUProfile() // returns once the profile is written
	closeFiles(p.cpuOut)
	p.settle(p.cpuOut, nil)
	p.cpuOut = nil
}

// writeMem writes the allocation profile into fs and settles them (under
// io).
func (p *profiler) writeMem(fs []*profFile) {
	w, err := open(fs)
	if err == nil {
		err = pprof.Lookup("allocs").WriteTo(w, 0)
		closeFiles(fs)
	}
	p.settle(fs, err)
}

// listingDone is listing.Config.ListingDone: it marks the boundary now and
// switches the profiles to the tail on a goroutine of its own, so the
// order stage, which plans the drain's segments once this returns, does
// not wait for the listing phase's profile to be written.
func (p *profiler) listingDone() {
	at := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || !p.boundary.IsZero() {
		return
	}
	p.boundary, p.switched = at, make(chan struct{})
	// The switch's files are planned now, so an account from here on
	// names them.
	var tail, mem []*profFile
	if p.cpu != "" {
		tail = p.planLocked(kindCPUTail, p.cpu+phaseTail)
	}
	if p.mem != "" {
		mem = p.planLocked(kindMemListing, p.mem+phaseListing)
	}
	go p.switchPhase(at, tail, mem, p.switched)
}

// errSwitchLate settles the switch's files when the run ended first.
var errSwitchLate = errors.New("not written: the run ended before the phase switch")

// switchPhase stops the listing phase's profiles and starts the tail's:
// the CPU profile into tail, the allocation profile into mem.
func (p *profiler) switchPhase(at time.Time, tail, mem []*profFile, done chan struct{}) {
	defer close(done)
	p.io.Lock()
	defer p.io.Unlock()
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		p.settle(append(tail, mem...), errSwitchLate)
		return
	}
	if tail != nil {
		cut := time.Now()
		p.stopCPU()
		err := p.startCPU(tail)
		started := time.Now()
		p.mu.Lock()
		p.cut, p.cutDelay = true, cut.Sub(at)
		if err == nil {
			p.tail, p.gap = true, started.Sub(cut)
		}
		p.mu.Unlock()
	}
	if mem != nil {
		p.writeMem(mem)
	}
}

// wait waits for the phase switch listingDone started, if any. A nil
// profiler does not wait.
func (p *profiler) wait() {
	if p == nil {
		return
	}
	p.mu.Lock()
	ch := p.switched
	p.mu.Unlock()
	if ch != nil {
		<-ch
	}
}

// stop ends the profiles: the CPU profile's last phase, and the
// allocation profile at the end. Nothing starts after it.
func (p *profiler) stop() {
	if p == nil {
		return
	}
	p.wait()
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	exit := slices.Clone(p.files[kindMemExit])
	p.mu.Unlock()
	p.io.Lock()
	p.stopCPU()
	if exit != nil {
		p.writeMem(exit)
	}
	p.io.Unlock()
	p.wait() // a switch that began as stop did settles its files unwritten
}

// account is the run report's account of the profiles now (nil for a nil
// profiler): each file with its status, the boundary, and the switch's
// times once it has run. It never waits for a profile's write.
func (p *profiler) account() *listing.Profiles {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	a := &listing.Profiles{Files: []listing.ProfileFile{}}
	if !p.boundary.IsZero() {
		a.PhaseBoundaryAt = p.boundary.UTC()
	}
	if p.cut {
		cut := int64(p.cutDelay)
		a.CPUCutDelayNs = &cut
		if p.tail {
			gap := int64(p.gap)
			a.CPUGapNs = &gap
		}
	}
	if p.mem != "" {
		rate := p.rate
		a.MemProfileRate = &rate
	}
	for _, kind := range []string{kindCPUListing, kindCPUTail, kindMemListing, kindMemExit} {
		for _, f := range p.files[kind] {
			a.Files = append(a.Files, listing.ProfileFile{Kind: f.kind, Path: f.path, Status: f.status, Error: f.err})
		}
	}
	return a
}
