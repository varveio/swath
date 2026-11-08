// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command shapes runs the nightly shape suite: every bucket shape (package
// internal/shapes) at full scale behind the in-process fake with its
// service behaviours (arms.go), and writes one JSON summary. With
// -baseline it compares the summary with a committed one and reports every
// number that moved by more than -tolerance as a GitHub annotation (and a
// table in $GITHUB_STEP_SUMMARY); such moves do not fail the run. A wrong
// listing (rows that are not the shape's keys, an arm that fails or times
// out) does.
//
//	go run ./bench/shapes -out shapes.json -baseline bench/baselines/shapes.json
//	go run ./bench/shapes -run 'throttle|pq\.' -scale 200000 -out /tmp/s.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/shapes"
	"github.com/varveio/swath/go/internal/source/scalefake"
)

// Summary is the suite's output, and the baseline's format.
type Summary struct {
	Scale     int             `json:"scale"`
	GoVersion string          `json:"go_version"`
	CPUs      int             `json:"cpus"`
	Started   time.Time       `json:"started"`
	WallS     float64         `json:"wall_s"`
	Results   []shapes.Result `json:"results"`
	Failures  []string        `json:"failures,omitempty"`
}

func main() {
	scale := flag.Int("scale", shapes.Full, "shape scale (keys per shape, about)")
	run := flag.String("run", "", "run only the arms whose name matches this `regexp`")
	out := flag.String("out", "shapes.json", "write the summary here")
	base := flag.String("baseline", "", "compare with this summary `file` and annotate moves")
	tol := flag.Float64("tolerance", 0.2, "annotate a number that moved by more than this fraction")
	timeout := flag.Duration("arm-timeout", 15*time.Minute, "fail an arm that runs longer")
	scratch := flag.String("scratch", "", "directory for the Parquet arms' datasets (default: a temporary one)")
	list := flag.Bool("list", false, "print the arms and exit")
	flag.Parse()
	arms := suite()
	if *run != "" {
		re, err := regexp.Compile(*run)
		if err != nil {
			log.Fatal(err)
		}
		var keep []arm
		for _, a := range arms {
			if re.MatchString(a.name) {
				keep = append(keep, a)
			}
		}
		arms = keep
	}
	if *list {
		for _, a := range arms {
			fmt.Printf("%s\t%s\t%d workers\t%+v\t%q\n", a.name, a.shape, a.workers, a.fake, a.flags)
		}
		return
	}
	dir := *scratch
	if dir == "" {
		d, err := os.MkdirTemp("", "shapes-")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(d)
		dir = d
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	sum := Summary{Scale: *scale, GoVersion: runtime.Version(), CPUs: runtime.NumCPU(), Started: time.Now().UTC()}
	sort.SliceStable(arms, func(i, j int) bool { return shapeIndex(arms[i].shape) < shapeIndex(arms[j].shape) })
	var keys []string
	var ks *scalefake.Keyspace
	shape := ""
	for _, a := range arms {
		if a.shape != shape {
			s, ok := shapes.Lookup(a.shape)
			if !ok {
				log.Fatalf("%s: no shape %q", a.name, a.shape)
			}
			ks, keys = nil, nil
			runtime.GC()
			t := time.Now()
			keys = s.Keys(*scale)
			var err error
			if ks, err = scalefake.FromKeys(keys); err != nil {
				log.Fatal(err)
			}
			shape = a.shape
			log.Printf("shape %s: %d keys in %s", shape, len(keys), time.Since(t).Round(time.Millisecond))
		}
		r, err := runArm(ks, keys, a, *scale, dir, *timeout)
		if err == nil && (r.Rows != int64(len(keys)) || (r.Identical != nil && !*r.Identical)) {
			err = fmt.Errorf("%d rows of %d keys, identical %v", r.Rows, len(keys), r.Identical)
		}
		if err != nil {
			msg := fmt.Sprintf("%s: %v", a.name, err)
			sum.Failures = append(sum.Failures, msg)
			fmt.Printf("::error title=shape arm failed::%s\n", msg)
			continue
		}
		sum.Results = append(sum.Results, r)
		log.Printf("%-36s wall %6d ms  ideal %8.0f ms  x%5.2f  req/1k %5.2f  att/1k %5.2f  depth %3d  active %5.1f  splits %4d  throttles %d",
			r.Name, r.WallMs, r.IdealMs, r.Ratio, r.RequestsPer1k, r.AttemptsPer1k, r.Depth, r.MeanActive, r.Splits, r.Throttles)
	}
	sum.WallS = time.Since(sum.Started).Seconds()
	b, err := json.MarshalIndent(sum, "", " ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("%d arms, %d failed, %.0f s; summary in %s", len(sum.Results), len(sum.Failures), sum.WallS, *out)
	if *base != "" {
		if err := compare(*base, sum, *tol); err != nil {
			log.Fatal(err)
		}
	}
	if len(sum.Failures) > 0 {
		os.Exit(1)
	}
}

func shapeIndex(name string) int {
	for i, s := range shapes.All {
		if s.Name == name {
			return i
		}
	}
	return len(shapes.All)
}

func runArm(ks *scalefake.Keyspace, keys []string, a arm, scale int, dir string, timeout time.Duration) (shapes.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	sa := shapes.Arm{Name: a.name, Shape: a.shape, Scale: scale, Workers: a.workers, Fake: a.fake, Flags: a.flags, RTT: a.rtt}
	if a.parquet {
		sa.Out = filepath.Join(dir, a.name)
		defer os.RemoveAll(sa.Out)
	}
	r, err := shapes.Run(ctx, ks, keys, sa)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("over %s: %w", timeout, err)
	}
	return r, err
}

// metric is one compared number, with the least change that counts
// whatever the fraction (a depth of 3 that becomes 4 is noise).
type metric struct {
	name  string
	get   func(shapes.Result) float64
	floor float64
}

var metrics = []metric{
	{"wall/ideal", func(r shapes.Result) float64 { return r.Ratio }, 0.1},
	{"requests/1k", func(r shapes.Result) float64 { return r.RequestsPer1k }, 0.05},
	{"attempts/1k", func(r shapes.Result) float64 { return r.AttemptsPer1k }, 0.05},
	{"depth", func(r shapes.Result) float64 { return float64(r.Depth) }, 3},
	{"mean active", func(r shapes.Result) float64 { return r.MeanActive }, 2},
}

// compare annotates every metric of every arm that moved by more than tol
// against the baseline, and lists arms only on one side.
func compare(path string, sum Summary, tol float64) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("::notice title=shape baseline::no baseline at %s; commit this run's summary to start one\n", path)
		return nil
	}
	if err != nil {
		return err
	}
	var base Summary
	if err := json.Unmarshal(b, &base); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if base.Scale != sum.Scale {
		fmt.Printf("::notice title=shape baseline::baseline scale %d, this run %d: not compared\n", base.Scale, sum.Scale)
		return nil
	}
	old := map[string]shapes.Result{}
	for _, r := range base.Results {
		old[r.Name] = r
	}
	var rows []string
	for _, r := range sum.Results {
		o, ok := old[r.Name]
		if !ok {
			rows = append(rows, fmt.Sprintf("| %s | (new arm) | | | |", r.Name))
			continue
		}
		delete(old, r.Name)
		for _, m := range metrics {
			was, now := m.get(o), m.get(r)
			if math.Abs(now-was) < m.floor || math.Abs(now-was) <= tol*math.Abs(was) {
				continue
			}
			ch := "n/a"
			if was != 0 {
				ch = fmt.Sprintf("%+.0f %%", (now-was)/was*100)
			}
			fmt.Printf("::warning title=shape %s moved::%s %s %.3g -> %.3g (%s) against %s\n", r.Name, r.Name, m.name, was, now, ch, path)
			rows = append(rows, fmt.Sprintf("| %s | %s | %.3g | %.3g | %s |", r.Name, m.name, was, now, ch))
		}
	}
	for name := range old {
		rows = append(rows, fmt.Sprintf("| %s | (not run) | | | |", name))
	}
	sort.Strings(rows)
	report := fmt.Sprintf("### Shape suite against %s\n\n%d arms, %d failed, %.0f s at scale %d; numbers that moved more than %.0f %%:\n\n",
		path, len(sum.Results), len(sum.Failures), sum.WallS, sum.Scale, tol*100)
	if len(rows) == 0 {
		report += "none.\n"
	} else {
		report += "| arm | metric | baseline | now | change |\n|---|---|---|---|---|\n" + strings.Join(rows, "\n") + "\n"
	}
	fmt.Print(report)
	if f := os.Getenv("GITHUB_STEP_SUMMARY"); f != "" {
		w, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			return err
		}
		defer w.Close()
		_, err = io.WriteString(w, report)
		return err
	}
	return nil
}
