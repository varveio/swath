// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
)

// nofileEnv lowers the soft descriptor limit of a served child before it
// starts: the Go runtime raises the soft limit to the hard one at startup,
// so the child lowers it again itself, as a host whose hard limit is low
// would leave it.
const nofileEnv = "SWATH_REPLAY_TEST_NOFILE"

func init() {
	v := os.Getenv(nofileEnv)
	if v == "" {
		return
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		panic(err)
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		panic(err)
	}
	limit.Cur = n
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		panic(err)
	}
}

// TestServeWithinADescriptorLimit serves a fixture of 120 parts from a
// process whose descriptor limit is 64, so the descriptor cache holds at
// most 32 parts open, while 12 clients walk the whole listing at once in
// pages that cross parts. Every walk lists every key, the process's open
// descriptors (sampled from /proc) stay under the limit, and the metrics
// show the cache bounded (handle_cache.peak at most 32) and evicting.
func TestServeWithinADescriptorLimit(t *testing.T) {
	const parts, rows, limit, walks = 120, 100, 64, 12
	dir := t.TempDir()
	for p := range parts {
		var rs []legacytest.Row
		for r := range rows {
			i := p*rows + r
			rs = append(rs, legacytest.Object(fmt.Sprintf("logs/%05d", i), i))
		}
		part := legacytest.Part{Rows: rs, Stamps: legacytest.Stamps(p+1, p == parts-1)}
		if err := legacytest.Write(filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", p+1)), part); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(os.Args[0], "serve", "--fixture", dir, "--bucket", "bkt", "--port", "0", "--metrics-port", "0",
		"--parquet-connections", "4", "--max-concurrent-requests", "16")
	cmd.Env = append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1", nofileEnv+"="+strconv.Itoa(limit))
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	var startup string
	lines := bufio.NewScanner(stderr)
	for lines.Scan() {
		if strings.HasPrefix(lines.Text(), "swath_replay endpoint=") {
			startup = lines.Text()
			break
		}
	}
	go io.Copy(io.Discard, stderr)
	if startup == "" {
		t.Fatal("no startup line")
	}
	endpoint := regexp.MustCompile(`endpoint=(http://[^ ]+)`).FindStringSubmatch(startup)[1]
	metricsURL := regexp.MustCompile(`metrics_endpoint=(http://[^ ]+)`).FindStringSubmatch(startup)[1]

	var fdPeak atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		fdDir := fmt.Sprintf("/proc/%d/fd", cmd.Process.Pid)
		for {
			if fds, err := os.ReadDir(fdDir); err == nil {
				fdPeak.Store(max(fdPeak.Load(), int64(len(fds))))
			}
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	client := &http.Client{Transport: &http.Transport{Proxy: nil, MaxIdleConnsPerHost: walks}, Timeout: time.Minute}
	var wg sync.WaitGroup
	for range walks {
		wg.Go(func() {
			keys, token := 0, ""
			for {
				target := endpoint + "/bkt?list-type=2&max-keys=250"
				if token != "" {
					target += "&continuation-token=" + url.QueryEscape(token)
				}
				resp, err := client.Get(target)
				if err != nil {
					t.Error(err)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Errorf("%d: %s", resp.StatusCode, body)
					return
				}
				keys += strings.Count(string(body), "<Key>")
				m := regexp.MustCompile(`<NextContinuationToken>([^<]*)<`).FindStringSubmatch(string(body))
				if m == nil {
					break
				}
				token = m[1]
			}
			if keys != parts*rows {
				t.Errorf("walk listed %d keys, want %d", keys, parts*rows)
			}
		})
	}
	wg.Wait()
	close(stop)
	<-sampled
	var payload struct {
		Meters []struct {
			Name  string   `json:"name"`
			Count *float64 `json:"count"`
			Value *float64 `json:"value"`
		} `json:"meters"`
	}
	if err := json.Unmarshal([]byte(fetch(t, metricsURL)), &payload); err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, m := range payload.Meters {
		if m.Count != nil {
			got[m.Name] += *m.Count
		} else if m.Value != nil {
			got[m.Name] = *m.Value
		}
	}
	t.Logf("descriptors peak %d under a limit of %d; handle cache %v opens, %v evictions, %v waits, peak %v of %v",
		fdPeak.Load(), limit, got["swath.replay.handle_cache.opens"], got["swath.replay.handle_cache.evictions"],
		got["swath.replay.handle_cache.waits"], got["swath.replay.handle_cache.peak"], got["swath.replay.handle_cache.limit"])
	if fdPeak.Load() >= limit {
		t.Errorf("%d descriptors open, at the limit of %d", fdPeak.Load(), limit)
	}
	if got["swath.replay.handle_cache.limit"] != limit/2 || got["swath.replay.handle_cache.peak"] > limit/2 ||
		got["swath.replay.handle_cache.evictions"] == 0 || got["swath.replay.http.errors"] != 0 {
		t.Errorf("meters: %v", got)
	}
}
