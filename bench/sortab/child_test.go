// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/listing"
)

// The test binary is also the harness: SORTAB_CHILD=<config.json> runs one
// attempt of one arm (a child the driver can SIGKILL), SORTAB_DRIVE=<cell
// .json> drives one cell (its attempts, the verification) and appends the
// result as a JSON line to SORTAB_OUT. Otherwise the unit tests run.
func TestMain(m *testing.M) {
	if p := os.Getenv("SORTAB_CHILD"); p != "" {
		if err := child(p); err != nil {
			fmt.Fprintln(os.Stderr, "sortab child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if p := os.Getenv("SORTAB_DRIVE"); p != "" {
		if err := driveFile(p, os.Getenv("SORTAB_OUT")); err != nil {
			fmt.Fprintln(os.Stderr, "sortab drive:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// childConfig is one attempt.
type childConfig struct {
	Arm     string `json:"arm"` // "A" or "B"
	Shape   Shape  `json:"shape"`
	Limits  Limits `json:"limits"`
	KillAt  int64  `json:"kill_at"` // SIGKILL after this many keys listed (0: none)
	Resume  bool   `json:"resume"`
	Out     string `json:"out"`     // the dataset
	Staging string `json:"staging"` // B's runs and checkpoint
	Pages   string `json:"pages"`   // page log
	Status  string `json:"status"`  // the last status sample
	Result  string `json:"result"`  // written on completion
}

// sample is a child's live state, rewritten every sampleEvery: what the
// driver knows of an attempt that was killed.
type sample struct {
	Ms       int64 `json:"ms"`
	Listed   int64 `json:"listed"`
	Requests int64 `json:"requests"`
	Encoded  int64 `json:"encoded"`
	Parts    int64 `json:"parts"` // parts published (durable)
	// FirstDurableMs: A's first journaled part, B's first durable run;
	// FirstPartMs: the first published part in either arm (-1: none yet).
	FirstDurableMs int64 `json:"first_durable_ms"`
	FirstPartMs    int64 `json:"first_part_ms"`
	// Encoder activity: samples in which the encoded count advanced, of
	// all samples (the encoders' busy share of the attempt, sampled).
	EncodeActive int64 `json:"encode_active"`
	Samples      int64 `json:"samples"`
	// A's order-stage peaks (progress gauges); B's staging.
	PeakNodes   int64 `json:"peak_nodes"`
	PeakMarkers int64 `json:"peak_markers"`
	PeakReaders int64 `json:"peak_readers"`
	PeakSpill   int64 `json:"peak_spill_bytes"`
	SpillLive   int64 `json:"spill_live_bytes"`
	// /proc/self/io: bytes read and written through syscalls (spill,
	// runs, parts, journal), and the bytes that reached the block layer.
	Rchar      int64 `json:"rchar"`
	Wchar      int64 `json:"wchar"`
	ReadBytes  int64 `json:"read_bytes"`
	WriteBytes int64 `json:"write_bytes"`
	// HWMKB is the process's peak resident set (VmHWM): its own address
	// space only. wait4's ru_maxrss also carries the parent's RSS at the
	// fork, which exec does not reset.
	HWMKB int64 `json:"hwm_kb"`
}

const sampleEvery = 50 * time.Millisecond

// childResult is a completed attempt.
type childResult struct {
	Arm    string          `json:"arm"`
	Error  string          `json:"error,omitempty"`
	WallMs int64           `json:"wall_ms"`
	A      *listing.Report `json:"a,omitempty"`
	B      *BStats         `json:"b,omitempty"`
	Last   sample          `json:"last"`
}

func child(path string) error {
	var cfg childConfig
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return err
	}
	p, err := NewProvider(cfg.Shape, cfg.Pages)
	if err != nil {
		return err
	}
	p.KillAt = cfg.KillAt
	start := time.Now()
	var smp sample
	smp.FirstDurableMs, smp.FirstPartMs = -1, -1
	var encoded, parts atomic.Int64 // B's
	var firstRun atomic.Int64       // B's first durable run, ms (-1: none)
	firstRun.Store(-1)
	pr := &listing.Progress{}
	read := func() {
		now := time.Since(start).Milliseconds()
		smp.Ms, smp.Listed, smp.Requests, smp.Samples = now, p.Listed(), p.Requests(), smp.Samples+1
		var enc, pub int64
		if cfg.Arm == "A" {
			s := pr.Snapshot()
			enc, pub = s.Encoded, s.Parts
			if o := s.Order; o != nil {
				smp.PeakNodes, smp.PeakMarkers, smp.PeakReaders, smp.PeakSpill, smp.SpillLive = o.PeakNodes, o.PeakMarkers, o.PeakSpillReaders, o.PeakSpillBytes, o.SpillLiveBytes
			}
			if pub > 0 && smp.FirstDurableMs < 0 {
				smp.FirstDurableMs = now
			}
		} else {
			enc, pub = encoded.Load(), parts.Load()
			if f := firstRun.Load(); f >= 0 && smp.FirstDurableMs < 0 {
				smp.FirstDurableMs = f
			}
		}
		if enc > smp.Encoded {
			smp.EncodeActive++
		}
		smp.Encoded, smp.Parts = enc, pub
		if pub > 0 && smp.FirstPartMs < 0 {
			smp.FirstPartMs = now
		}
		smp.Rchar, smp.Wchar, smp.ReadBytes, smp.WriteBytes = procIO()
		smp.HWMKB = vmHWM()
	}
	write := func() { _ = writeJSON(cfg.Status, smp) }
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		tk := time.NewTicker(sampleEvery)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				read()
				write()
			}
		}
	}()
	res := childResult{Arm: cfg.Arm}
	ctx := context.Background()
	var runErr error
	switch cfg.Arm {
	case "A":
		rep, err := runA(ctx, p, cfg.Out, cfg.Limits, cfg.Resume, pr)
		res.A, runErr = &rep, err
	case "B":
		st, err := runBWatched(ctx, p, cfg, &encoded, &parts, &firstRun)
		res.B, runErr = &st, err
	default:
		return fmt.Errorf("arm %q", cfg.Arm)
	}
	close(stop)
	<-sampled
	read()
	write()
	res.WallMs = time.Since(start).Milliseconds()
	res.Last = smp
	if runErr != nil {
		res.Error = runErr.Error()
	}
	if err := writeJSON(cfg.Result, res); err != nil {
		return err
	}
	return runErr
}

// runBWatched runs arm B, publishing its first durable run's time.
func runBWatched(ctx context.Context, p *Provider, cfg childConfig, encoded, parts, firstRun *atomic.Int64) (BStats, error) {
	start := time.Now()
	stop := make(chan struct{})
	defer close(stop)
	go func() { // a run is durable once the checkpoint names it
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
			}
			if ck, err := loadCheckpoint(cfg.Staging); err == nil && len(ck.Runs) > 0 {
				firstRun.Store(time.Since(start).Milliseconds())
				return
			}
		}
	}()
	return runB(ctx, p, cfg.Out, cfg.Staging, cfg.Limits, cfg.Resume, encoded, parts)
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func procIO() (rchar, wchar, readBytes, writeBytes int64) {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ": ")
		if !ok {
			continue
		}
		n, _ := strconv.ParseInt(v, 10, 64)
		switch k {
		case "rchar":
			rchar = n
		case "wchar":
			wchar = n
		case "read_bytes":
			readBytes = n
		case "write_bytes":
			writeBytes = n
		}
	}
	return
}

// vmHWM is /proc/self/status VmHWM in KiB (0 if unknown).
func vmHWM() int64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			n, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
			return n
		}
	}
	return 0
}
