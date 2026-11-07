// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	_ "time/tzdata" // the zones below, wherever the test runs

	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// serveWith starts this binary as swath-replay serve with extra
// environment and returns its endpoint.
func serveWith(t *testing.T, env []string, args ...string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"serve"}, args...)...)
	cmd.Env = append(append(os.Environ(), "SWATH_REPLAY_TEST_MAIN=1"), env...)
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	lines := bufio.NewScanner(stderr)
	for lines.Scan() {
		if m := regexp.MustCompile(`^swath_replay endpoint=(http://[^ ]+)`).FindStringSubmatch(lines.Text()); m != nil {
			go func() {
				for lines.Scan() {
				}
			}()
			return m[1]
		}
	}
	t.Fatal("no startup line")
	return ""
}

// TestTimesIgnoreTheLocalZone serves one Parquet fixture on all three
// routes under TZ=UTC and under two zones far from it (one ahead by
// 12:45 or 13:45 across a daylight change, one behind by 3:30): every
// rendered time (S3 LastModified, GCS updated, Azure Last-Modified) is
// the same UTC instant, byte for byte, the replay wire contract's rule
// that the fixture's instant is rendered in UTC whatever the server's
// zone.
func TestTimesIgnoreTheLocalZone(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the TZ variable")
	}
	dir := t.TempDir()
	var rows []sortedparquettest.Row
	// Around the epoch, a daylight change in both zones, a year boundary
	// in UTC that is another year locally, and a pre-epoch instant.
	for i, micros := range []int64{0, -1, 1_696_082_400_000_000 - 1, 1_696_082_400_000_000, 1_767_225_599_999_999, 1_767_225_600_000_000,
		-86_400_000_001, 1_712_440_800_123_456} {
		r := sortedparquettest.Object("k"+string(rune('a'+i)), i)
		m := micros
		r.LastModified = &m
		rows = append(rows, r)
	}
	if err := sortedparquettest.Write(filepath.Join(dir, "part-00001.parquet"),
		sortedparquettest.Part{Rows: rows, Stamps: sortedparquettest.Stamps(1, true)}); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		"/bkt?list-type=2&max-keys=100",
		"/storage/v1/b/bkt/o?maxResults=100",
		"/replay/bkt?restype=container&comp=list&maxresults=100",
	}
	answers := func(tz string) []string {
		endpoint := serveWith(t, []string{"TZ=" + tz}, "--fixture", dir, "--bucket", "bkt", "--port", "0",
			"--protocols", "s3,gcs,azure", "--azure-account", "replay", "--parquet-connections", "2")
		var out []string
		for _, p := range paths {
			req, _ := http.NewRequest("GET", endpoint+p, nil)
			req.Header.Set("x-ms-version", "2026-06-06")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			b := new(strings.Builder)
			_, _ = bufio.NewReader(resp.Body).WriteTo(b)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("TZ=%s %s: %d %s", tz, p, resp.StatusCode, b)
			}
			// The ServiceEndpoint names the port, which differs per run.
			out = append(out, regexp.MustCompile(`http://[^/"]+/`).ReplaceAllString(b.String(), "http://endpoint/"))
		}
		return out
	}
	utc := answers("UTC")
	for _, want := range []string{"<LastModified>1970-01-01T00:00:00.000Z</LastModified>", `"updated":"1970-01-01T00:00:00.000000Z"`,
		"<Last-Modified>Thu, 01 Jan 1970 00:00:00 GMT</Last-Modified>"} {
		if !strings.Contains(strings.Join(utc, ""), want) {
			t.Errorf("UTC answers lack %s", want)
		}
	}
	for _, tz := range []string{"Pacific/Chatham", "America/St_Johns"} {
		got := answers(tz)
		for i := range paths {
			if got[i] != utc[i] {
				t.Errorf("TZ=%s %s differs from UTC:\n%s\n%s", tz, paths[i], got[i], utc[i])
			}
		}
	}
}
