// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/varveio/swath/go/internal/listing"
)

// The profiles' phase files beside FILE: the listing phase (from the start
// to the end of listing), the tail after it (CPU) and the process's end
// (memory). A side-path check covers each (checkSidePaths).
const (
	phaseListing = ".listing"
	phaseTail    = ".tail"
	phaseExit    = ".exit"
)

// The run report's kinds of profile file (listing.ProfileFile).
const (
	kindCPUListing = "cpu_listing"
	kindCPUTail    = "cpu_tail"
	kindMemListing = "mem_listing"
	kindMemExit    = "mem_exit"
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
// start to that boundary into FILE and FILE.listing (the same bytes: FILE
// is the listing phase, no longer the whole run, since Go runs one CPU
// profile at a time and keeps no whole-run profile beside the phases'), and
// from the boundary to the end into FILE.tail; go tool pprof FILE.listing
// FILE.tail merges them. The allocation profile, cumulative, is written at
// the boundary into FILE.listing and at the end into FILE.exit and FILE
// (the whole run, as before the split); go tool pprof -base FILE.listing
// FILE.exit is the tail's. Without a boundary (listing never finished, a
// dry run) the listing-phase CPU files hold the whole run and no tail or
// memory listing file is written. A nil profiler (neither variable set)
// does nothing.
type profiler struct {
	cpu, mem string // the resolved paths (checkSidePaths); "" off
	rate     int    // runtime.MemProfileRate with mem
	warn     func(string)

	mu       sync.Mutex
	cpuOut   []*os.File    // the running CPU profile's files; nil: none running
	boundary time.Time     // when listing finished (zero: not yet, or never)
	switched chan struct{} // closed once the phase switch listingDone started has run
	tail     bool          // the tail's CPU profile started
	gap      time.Duration // from the boundary to its start
	memDone  bool          // the allocation profile's listing file attempted
	stopped  bool          // stop ran: no phase starts after it
}

// startProfiles starts the profiles cpu and mem name ("" off; resolved by
// checkSidePaths), with runtime.MemProfileRate rate while mem is on. It
// returns nil when both are off.
func startProfiles(cpu, mem string, rate int, warn func(string)) (*profiler, error) {
	if cpu == "" && mem == "" {
		return nil, nil
	}
	p := &profiler{cpu: cpu, mem: mem, rate: rate, warn: warn}
	if mem != "" {
		runtime.MemProfileRate = rate
	}
	if cpu != "" {
		if err := p.startCPU(cpu, cpu+phaseListing); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// startCPU starts the CPU profile into the files paths name.
func (p *profiler) startCPU(paths ...string) error {
	var files []*os.File
	ws := make([]io.Writer, 0, len(paths))
	closeAll := func() {
		for _, f := range files {
			f.Close()
		}
	}
	for _, path := range paths {
		f, err := createNoFollow(path)
		if err != nil {
			closeAll()
			return err
		}
		files = append(files, f)
		ws = append(ws, f)
	}
	if err := pprof.StartCPUProfile(io.MultiWriter(ws...)); err != nil {
		closeAll()
		return err
	}
	p.cpuOut = files
	return nil
}

// stopCPU stops the running CPU profile, if any, and closes its files.
func (p *profiler) stopCPU() {
	if p.cpuOut == nil {
		return
	}
	pprof.StopCPUProfile() // returns once the profile is written
	for _, f := range p.cpuOut {
		if err := f.Close(); err != nil {
			p.warnf("profile %s: %v", f.Name(), err)
		}
	}
	p.cpuOut = nil
}

// writeMem writes the allocation profile into the files paths name.
func (p *profiler) writeMem(paths ...string) {
	var files []*os.File
	ws := make([]io.Writer, 0, len(paths))
	for _, path := range paths {
		f, err := createNoFollow(path)
		if err != nil {
			p.warnf("profile %s: %v", path, err)
			continue
		}
		files = append(files, f)
		ws = append(ws, f)
	}
	if len(files) == 0 {
		return
	}
	err := pprof.Lookup("allocs").WriteTo(io.MultiWriter(ws...), 0)
	for _, f := range files {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		p.warnf("profile %s: %v", p.mem, err)
	}
}

func (p *profiler) warnf(format string, a ...any) {
	if p.warn != nil {
		p.warn(fmt.Sprintf(format, a...))
	}
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
	go func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		defer close(p.switched)
		if p.stopped {
			return
		}
		if p.cpu != "" {
			p.stopCPU()
			if err := p.startCPU(p.cpu + phaseTail); err != nil {
				p.warnf("profile %s: %v", p.cpu+phaseTail, err)
			} else {
				p.tail, p.gap = true, time.Since(at)
			}
		}
		if p.mem != "" {
			p.writeMem(p.mem + phaseListing)
			p.memDone = true
		}
	}()
}

// wait waits for the phase switch listingDone started, if any.
func (p *profiler) wait() {
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
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.stopped = true
	p.stopCPU()
	if p.mem != "" {
		p.writeMem(p.mem+phaseExit, p.mem)
	}
}

// account is the run report's account of the profiles (nil for a nil
// profiler): the files by phase and the boundary. It waits for a phase
// switch under way, so the gap it reports is the switch's.
func (p *profiler) account() *listing.Profiles {
	if p == nil {
		return nil
	}
	p.wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	a := &listing.Profiles{Files: []listing.ProfileFile{}}
	if !p.boundary.IsZero() {
		a.PhaseBoundaryAt = p.boundary.UTC()
	}
	add := func(kind string, paths ...string) {
		for _, path := range paths {
			a.Files = append(a.Files, listing.ProfileFile{Kind: kind, Path: path})
		}
	}
	if p.cpu != "" {
		add(kindCPUListing, p.cpu, p.cpu+phaseListing)
		if p.tail {
			add(kindCPUTail, p.cpu+phaseTail)
			gap := int64(p.gap)
			a.CPUGapNs = &gap
		}
	}
	if p.mem != "" {
		a.MemProfileRate = p.rate
		if p.memDone {
			add(kindMemListing, p.mem+phaseListing)
		}
		add(kindMemExit, p.mem+phaseExit, p.mem)
	}
	return a
}
