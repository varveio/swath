// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Cell is one measured configuration: an arm on a shape under limits,
// uninterrupted (KillFrac 0) or SIGKILLed and then resumed to completion:
// once KillFrac of the slow range's keys have been listed (slowfirst,
// slowlast: 0.5 is peak speculation, the siblings running ahead), else of
// all keys.
type Cell struct {
	Label    string  `json:"label"`
	Arm      string  `json:"arm"`
	Shape    Shape   `json:"shape"`
	Limits   Limits  `json:"limits"`
	KillFrac float64 `json:"kill_frac"`
	Rep      int     `json:"rep"`
	Dir      string  `json:"dir"`  // the cell's work directory (removed after, except its logs)
	Keep     bool    `json:"keep"` // keep the dataset and staging
}

// Attempt is one child process, as the driver saw it.
type Attempt struct {
	WallMs   int64        `json:"wall_ms"`
	UserMs   int64        `json:"user_ms"`
	SysMs    int64        `json:"sys_ms"`
	MaxRSSKB int64        `json:"max_rss_kb"`   // the child's VmHWM (its last sample)
	RuMaxRSS int64        `json:"ru_maxrss_kb"` // wait4's, which includes the parent's RSS at the fork
	Killed   bool         `json:"killed"`
	Exit     int          `json:"exit"`
	Requests int64        `json:"requests"` // pages in its log
	Keys     int64        `json:"keys"`     // keys in those pages (with repeats)
	Last     sample       `json:"last"`     // its last status sample
	Result   *childResult `json:"result,omitempty"`
	Stderr   string       `json:"stderr,omitempty"`
}

// CellResult is a cell's measurements and checks, one JSON line.
type CellResult struct {
	Cell
	Host     string    `json:"host"`
	Started  time.Time `json:"started"`
	Attempts []Attempt `json:"attempts"`
	// Completion plus recovery: the sums over attempts.
	WallMs   int64   `json:"wall_ms"`
	CPUMs    int64   `json:"cpu_ms"`
	CPUNsRow float64 `json:"cpu_ns_per_row"`
	PeakRSS  int64   `json:"peak_rss_kb"`
	Requests int64   `json:"requests"`
	// The resume: keys listed again (listed by the killed attempt and by
	// the resume), the resume's requests and keys, and its wall.
	RelistedKeys   int64 `json:"relisted_keys"`
	ResumeRequests int64 `json:"resume_requests"`
	ResumeKeys     int64 `json:"resume_keys"`
	RestartMs      int64 `json:"restart_ms"`
	// At the kill: keys listed (logged pages), rows durable in published parts (A) or checkpointed runs (B).
	DurableAtKill int64 `json:"durable_rows_at_kill"`
	ListedAtKill  int64 `json:"listed_at_kill"`
	// Exact identity and strict adjacency of the final dataset.
	Verified  bool   `json:"verified"`
	VerifyErr string `json:"verify_error,omitempty"`
	Rows      int64  `json:"rows"`
	Parts     int    `json:"parts"`
	PartRows  int64  `json:"max_part_rows"`
	Bytes     int64  `json:"dataset_bytes"`
	Error     string `json:"error,omitempty"`
}

func driveFile(cellPath, outPath string) error {
	b, err := os.ReadFile(cellPath)
	if err != nil {
		return err
	}
	var c Cell
	if err := json.Unmarshal(b, &c); err != nil {
		return err
	}
	res := drive(c, os.Args[0])
	line, err := json.Marshal(res)
	if err != nil {
		return err
	}
	if outPath == "" {
		_, err = fmt.Println(string(line))
		return err
	}
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if res.Error != "" {
		return errors.New(res.Error)
	}
	return nil
}

// drive runs a cell's attempts as child processes of bin (this test
// binary), then verifies the dataset.
func drive(c Cell, bin string) CellResult {
	res := CellResult{Cell: c, Started: time.Now().UTC()}
	res.Host, _ = os.Hostname()
	fail := func(err error) CellResult {
		res.Error = err.Error()
		return res
	}
	l, err := c.Shape.layout()
	if err != nil {
		return fail(err)
	}
	if err := os.RemoveAll(c.Dir); err != nil {
		return fail(err)
	}
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return fail(err)
	}
	out, staging := filepath.Join(c.Dir, "out"), filepath.Join(c.Dir, "staging")
	if !c.Keep {
		defer os.RemoveAll(out)
		defer os.RemoveAll(staging)
	}
	var logs [][]interval
	for n := 1; ; n++ {
		cfg := childConfig{Arm: c.Arm, Shape: c.Shape, Limits: c.Limits, Resume: n > 1, Out: out, Staging: staging,
			Pages: filepath.Join(c.Dir, fmt.Sprintf("pages-%d.log", n)), Status: filepath.Join(c.Dir, fmt.Sprintf("status-%d.json", n)),
			Result: filepath.Join(c.Dir, fmt.Sprintf("result-%d.json", n))}
		if n == 1 && c.KillFrac > 0 {
			n := c.Shape.Keys
			if l.slow >= 0 {
				n = c.Shape.SlowKeys
			}
			cfg.KillAt = max(1, int64(c.KillFrac*float64(n)))
		}
		at, err := attempt(bin, cfg, filepath.Join(c.Dir, fmt.Sprintf("config-%d.json", n)))
		if err != nil {
			return fail(err)
		}
		iv, err := readPages(cfg.Pages)
		if err != nil {
			return fail(err)
		}
		logs = append(logs, iv)
		at.Requests = int64(len(iv))
		for _, v := range iv {
			at.Keys += v.hi - v.lo
		}
		res.Attempts = append(res.Attempts, at)
		if at.Killed {
			if cfg.KillAt == 0 || n > 1 {
				return fail(fmt.Errorf("attempt %d killed unasked", n))
			}
			res.ListedAtKill = at.Keys // the pages it logged, the killing one included
			continue
		}
		if at.Exit != 0 {
			return fail(fmt.Errorf("attempt %d exit %d: %s", n, at.Exit, at.Stderr))
		}
		if cfg.KillAt > 0 {
			return fail(fmt.Errorf("attempt 1 completed before its kill at %d keys", cfg.KillAt))
		}
		break
	}
	for _, a := range res.Attempts {
		res.WallMs += a.WallMs
		res.CPUMs += a.UserMs + a.SysMs
		res.PeakRSS = max(res.PeakRSS, a.MaxRSSKB)
		res.Requests += a.Requests
	}
	if len(res.Attempts) == 2 {
		r := res.Attempts[1]
		res.ResumeRequests, res.ResumeKeys, res.RestartMs = r.Requests, r.Keys, r.WallMs
		res.RelistedKeys = overlap(logs[0], logs[1])
		if rr := r.Result; rr != nil {
			switch {
			case rr.A != nil:
				res.DurableAtKill = rr.A.Output.RowsRetained
			case rr.B != nil:
				res.DurableAtKill = rr.B.RunRowsResumed
			}
		}
	}
	v, err := verify(out, l)
	res.Rows, res.Parts, res.PartRows, res.Bytes = v.rows, v.parts, v.maxPartRows, v.bytes
	if err != nil {
		res.VerifyErr = err.Error()
		res.Error = "verification: " + err.Error()
	} else {
		res.Verified = true
	}
	if res.Rows > 0 {
		res.CPUNsRow = float64(res.CPUMs) * 1e6 / float64(res.Rows)
	}
	return res
}

// attempt runs one child and waits for it.
func attempt(bin string, cfg childConfig, cfgPath string) (Attempt, error) {
	var at Attempt
	if err := writeJSON(cfgPath, cfg); err != nil {
		return at, err
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "SORTAB_CHILD="+cfgPath, "SORTAB_DRIVE=")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	start := time.Now()
	err := cmd.Run()
	at.WallMs = time.Since(start).Milliseconds()
	st := cmd.ProcessState
	if st == nil {
		return at, err
	}
	if ru, ok := st.SysUsage().(*syscall.Rusage); ok {
		at.UserMs = ru.Utime.Sec*1000 + int64(ru.Utime.Usec)/1000
		at.SysMs = ru.Stime.Sec*1000 + int64(ru.Stime.Usec)/1000
		at.RuMaxRSS = ru.Maxrss
	}
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
		at.Killed = true
	}
	at.Exit = st.ExitCode()
	at.Stderr = tail(stderr.String(), 2000)
	if b, err := os.ReadFile(cfg.Status); err == nil {
		_ = json.Unmarshal(b, &at.Last)
	}
	if b, err := os.ReadFile(cfg.Result); err == nil {
		var r childResult
		if json.Unmarshal(b, &r) == nil {
			at.Result = &r
			at.Last = r.Last
		}
	}
	at.MaxRSSKB = at.Last.HWMKB
	return at, nil
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

// interval is the global key indices [lo, hi) of one page.
type interval struct{ lo, hi int64 }

func readPages(path string) ([]interval, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []interval
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) != 3 {
			continue // a torn last line
		}
		lo, err1 := strconv.ParseInt(fs[1], 10, 64)
		hi, err2 := strconv.ParseInt(fs[2], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, interval{lo, hi})
	}
	return out, sc.Err()
}

// union merges intervals.
func union(iv []interval) []interval {
	s := slices.Clone(iv)
	slices.SortFunc(s, func(a, b interval) int { return int(a.lo - b.lo) })
	var out []interval
	for _, v := range s {
		if v.hi <= v.lo {
			continue
		}
		if n := len(out); n > 0 && v.lo <= out[n-1].hi {
			out[n-1].hi = max(out[n-1].hi, v.hi)
			continue
		}
		out = append(out, v)
	}
	return out
}

// overlap is the number of keys in both sets of pages.
func overlap(a, b []interval) int64 {
	ua, ub := union(a), union(b)
	var n int64
	for i, j := 0, 0; i < len(ua) && j < len(ub); {
		lo, hi := max(ua[i].lo, ub[j].lo), min(ua[i].hi, ub[j].hi)
		if hi > lo {
			n += hi - lo
		}
		if ua[i].hi < ub[j].hi {
			i++
		} else {
			j++
		}
	}
	return n
}
