// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// The durable spill's validation campaign (contract §6: a real
// S3-compatible backend and production key lists): the real binary,
// SIGKILLed at the durable spill's steps, after a part record, mid
// recovery replay, with unsynced state lost, and at 40 % and 70 % of the
// keys, then resumed. Each resume must give the uninterrupted dataset
// (byte-identical with --part-interval=0, row-identical with the interval
// on), reconcile (rows retained + rows listed = the dataset; objects
// emitted - filtered + replayed = rows listed), and list nothing a
// replayed certificate covers: no data request of any resume (a killed
// first resume included) starts in
// [max(cs, K), ce) of a certificate live in the ledger's prefix. Opt-in:
// TestDurableMinIO runs with SWATH_MINIO_ENDPOINT (go/bench/minio.sh),
// TestDurableCorpus with SWATH_CORPUS_ENDPOINT (go/bench/durable_corpus.sh);
// both skip without them, and both require SWATH_CAMPAIGN_KEYS (with
// SWATH_CAMPAIGN_MAX_KEYS: its first lines only): the uninterrupted
// dataset must list exactly that key file. The reuse arms (kills after
// seals) must replay certificates. SWATH_CAMPAIGN_RESULTS names a file each arm
// appends its result line to (go/docs/measurements.md, "Durable spill").

// campaignFrontierDelay is the skewed fixture's slow frontier (contract
// §6, "reuse"): a data request (fresh or continued) for keys below the key
// at 10 % of the list waits this long, so the ranges ahead spill and seal
// certificates the kills at 40 % and 70 % find live.
const campaignFrontierDelay = 200 * time.Millisecond

// firstKey is a ListObjectsV2 page's first key (as the service escaped it;
// the frontier's keys are plain).
var firstKey = regexp.MustCompile(`<Key>([^<]*)</Key>`)

// campaignInterval is the part interval of the interval-on mode: parts
// close by time within a run of seconds, so K advances while ranges spill.
const campaignInterval = "1s"

// TestDurableMinIO runs the campaign against a MinIO bucket
// (SWATH_MINIO_ENDPOINT, SWATH_MINIO_BUCKET, SWATH_MINIO_ACCESS_KEY,
// SWATH_MINIO_SECRET_KEY; go/bench/minio.sh up starts and fills one).
func TestDurableMinIO(t *testing.T) {
	ep := os.Getenv("SWATH_MINIO_ENDPOINT")
	if ep == "" {
		t.Skip("SWATH_MINIO_ENDPOINT is not set (go/bench/minio.sh up)")
	}
	bucket := orDefault(os.Getenv("SWATH_MINIO_BUCKET"), "swath-durable")
	// 16 workers: at 64 concurrent listings the single-drive MinIO's heap
	// outgrew its container and requests timed out (go/bench/minio.sh).
	campaign(t, campaignFixture{name: "minio/" + bucket, upstream: ep, bucket: bucket, workers: orDefault(os.Getenv("SWATH_MINIO_WORKERS"), "16"), env: []string{
		"AWS_ACCESS_KEY_ID=" + orDefault(os.Getenv("SWATH_MINIO_ACCESS_KEY"), "swath"),
		"AWS_SECRET_ACCESS_KEY=" + orDefault(os.Getenv("SWATH_MINIO_SECRET_KEY"), "swath-secret"),
	}})
}

// TestDurableCorpus runs the campaign against a key list served by the
// bench's static fake (SWATH_CORPUS_ENDPOINT, SWATH_CORPUS_NAME;
// go/bench/durable_corpus.sh serves each list and runs it).
func TestDurableCorpus(t *testing.T) {
	ep := os.Getenv("SWATH_CORPUS_ENDPOINT")
	if ep == "" {
		t.Skip("SWATH_CORPUS_ENDPOINT is not set (go/bench/durable_corpus.sh)")
	}
	campaign(t, campaignFixture{name: orDefault(os.Getenv("SWATH_CORPUS_NAME"), "corpus"), upstream: ep, bucket: "bkt", anonymous: true})
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

type campaignFixture struct {
	name      string // the results' label
	upstream  string // the S3 endpoint
	bucket    string
	anonymous bool
	workers   string   // --workers ("": the default)
	keys      string   // the key file the fixture serves (the oracle)
	env       []string // the child's credentials
}

// campaignProxy fronts the fixture: it counts the keys served (the kills
// by key count), logs the start of each fresh data request while logging
// is on (the reuse assertion), and holds the slow frontier. It keeps the
// Host header, so a signed request still verifies upstream.
type campaignProxy struct {
	keys    atomic.Int64
	slow    atomic.Pointer[string] // the frontier's end (nil: no frontier)
	logging atomic.Bool
	mu      sync.Mutex
	starts  []string
}

func (p *campaignProxy) handler(up *url.URL) http.Handler {
	rp := httputil.NewSingleHostReverseProxy(up)
	rp.ErrorLog = log.New(io.Discard, "", 0) // a killed child's requests end canceled
	direct := rp.Director
	rp.Director = func(r *http.Request) {
		direct(r)
		r.Header.Del("Accept-Encoding") // count keys in plain bodies
	}
	rp.ModifyResponse = func(r *http.Response) error {
		if _, ok := r.Request.URL.Query()["list-type"]; !ok {
			return nil
		}
		b, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			return err
		}
		p.keys.Add(int64(bytes.Count(b, []byte("<Key>"))))
		r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(b)), int64(len(b))
		r.Header.Set("Content-Length", strconv.Itoa(len(b)))
		return nil
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		_, list := q["list-type"]
		if list && q.Get("delimiter") == "" && q.Get("continuation-token") == "" && p.logging.Load() {
			p.mu.Lock()
			p.starts = append(p.starts, q.Get("start-after"))
			p.mu.Unlock()
		}
		if !list || q.Get("delimiter") != "" {
			rp.ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		rp.ServeHTTP(rec, r)
		// A page below the frontier's end waits before it is answered.
		if s := p.slow.Load(); s != nil && rec.Body.Len() > 0 {
			if m := firstKey.FindSubmatch(rec.Body.Bytes()); m != nil && string(m[1]) < *s {
				time.Sleep(campaignFrontierDelay)
			}
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
	})
}

// campaignReport is the part of a run's JSON report the campaign reads.
type campaignReport struct {
	Output struct {
		RowsListed    int64 `json:"rows_listed"`
		RowsFiltered  int64 `json:"rows_filtered"`
		RowsRetained  int64 `json:"rows_retained"`
		RowsCommitted int64 `json:"rows_committed"`
		Bytes         int64 `json:"bytes"`
	} `json:"output"`
	Resources struct {
		Spill struct {
			Durable *order.DurableReport `json:"durable"`
		} `json:"spill"`
	} `json:"resources"`
	Source struct {
		Stats struct {
			Counters struct {
				Values map[string]any `json:"values"`
			} `json:"counters"`
		} `json:"stats"`
	} `json:"source"`
}

func (r *campaignReport) value(name string) int64 {
	f, _ := r.Source.Stats.Counters.Values[name].(float64)
	return int64(f)
}

// campaignArm is one kill schedule.
type campaignArm struct {
	name       string
	kill       string  // the list's SWATH_TEST_KILL_AT
	keys       float64 // or: SIGKILL once this fraction of the fixture's keys was served
	damage     func(t *testing.T, out string)
	resumeKill string // a first resume killed here (mid recovery replay)
	reuse      bool   // the kill falls after seals: the resume must replay certificates
}

var campaignArms = []campaignArm{
	{name: "capture-frames", kill: "spill-write-frames:1"}, // §3.2, §3.4
	{name: "seal", kill: "spill-seal:1"},                   // §3.4
	{name: "ledger-append", kill: "spill-ledger-append:1"}, // §3.5
	{name: "ledger-sync", kill: "spill-ledger-sync:1"},
	{name: "unlink", kill: "spill-unlink:1", reuse: true}, // §3.8
	{name: "part-recorded", kill: "recorded:2"},
	// Killed at the third ledger sync, two cycles of seals are durable:
	// certificates the resume replays, killed at its second replayed frame.
	{name: "mid-replay", kill: "spill-ledger-sync:3", resumeKill: "spill-replay-frame:2", reuse: true},
	{name: "lost-unsealed", kill: "spill-seal:2", damage: loseUnsealed},
	{name: "lost-ledger-tail", kill: "spill-ledger-sync:2", damage: tearLedgerTail},
	{name: "keys-40", keys: 0.4, reuse: true},
	{name: "keys-70", keys: 0.7, reuse: true},
}

// campaignRunner runs the binary for the campaign.
type campaignRunner struct {
	bin     string
	env     []string
	keys    *atomic.Int64 // the keys the fixture served (reset per run)
	timeout time.Duration // the watchdog: a run past it fails, whatever ended it
}

// run runs the binary with args. With killAt (SWATH_TEST_KILL_AT) the
// child must kill itself there: SIGKILLed, with the hook's marker on
// stderr. With keys, the harness's key-count killer must fire and the
// child die of it. Otherwise the run must succeed, and its report is
// returned. A run the watchdog ended fails in every case: its SIGKILL is
// not the requested fault.
func (c *campaignRunner) run(args []string, killAt string, keys int64, env ...string) (*campaignReport, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.bin, args...)
	cmd.Env = append(slices.Clone(c.env), env...)
	if killAt != "" {
		cmd.Env = append(cmd.Env, "SWATH_TEST_KILL_AT="+killAt)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	c.keys.Store(0)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	var fired atomic.Bool
	if keys > 0 {
		go func() {
			for {
				select {
				case <-done:
					return
				case <-time.After(time.Millisecond):
					if c.keys.Load() >= keys {
						fired.Store(true)
						cmd.Process.Signal(syscall.SIGKILL)
						return
					}
				}
			}
		}()
	}
	err := cmd.Wait()
	close(done)
	if ctx.Err() != nil {
		return nil, fmt.Errorf("watchdog: the run did not end within %s (the harness killed it; not the requested fault)\n%s", c.timeout, stderr.Bytes())
	}
	ws, _ := cmd.ProcessState.Sys().(syscall.WaitStatus)
	sigkilled := ws.Signaled() && ws.Signal() == syscall.SIGKILL
	switch {
	case killAt != "":
		if !sigkilled || !bytes.Contains(stderr.Bytes(), []byte(killMarker(killAt))) {
			return nil, fmt.Errorf("not killed at %s (not reached): %v\n%s", killAt, err, stderr.Bytes())
		}
		return nil, nil
	case keys > 0:
		if !sigkilled || !fired.Load() {
			return nil, fmt.Errorf("not killed at %d keys (fired %v): %v\n%s", keys, fired.Load(), err, stderr.Bytes())
		}
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("%v\n%s", err, stderr.Bytes())
	}
	var r campaignReport
	if err := json.Unmarshal(stdout.Bytes(), &r); err != nil {
		return nil, fmt.Errorf("report: %v", err)
	}
	return &r, nil
}

func campaign(t *testing.T, fx campaignFixture) {
	if fx.keys = os.Getenv("SWATH_CAMPAIGN_KEYS"); fx.keys == "" {
		t.Fatal("SWATH_CAMPAIGN_KEYS is required: the key file the fixture serves, the reference's independent oracle")
	}
	up, err := url.Parse(fx.upstream)
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	px := &campaignProxy{}
	srv := httptest.NewServer(px.handler(up))
	defer srv.Close()
	src := []string{"s3://" + fx.bucket, "--endpoint", srv.URL, "--path-style", "--region", "us-east-1"}
	if fx.anonymous {
		src = append(src, "--anonymous")
	}
	partSize := "2MiB"
	listArgsSized := func(out, size string, runtime []string) []string {
		a := append([]string{"diag", "list"}, src...) // diag: --spill-reserve
		a = append(a, "--part-size", size, "--row-budget", "1MiB", "--spill-max", "256MiB", "--report-format", "json", "-o", out)
		return append(a, runtime...)
	}
	listArgs := func(out string, runtime []string) []string { return listArgsSized(out, partSize, runtime) }
	rn := &campaignRunner{bin: bin, env: append(os.Environ(), fx.env...), keys: &px.keys, timeout: 20 * time.Minute}
	run := rn.run

	// The spill reserve is lowered from its 1 GiB default so whether the
	// fixture spills does not depend on the host's free disk.
	common := []string{"--spill-reserve", "64MiB"}
	if fx.workers != "" {
		common = append(common, "--workers", fx.workers)
	}
	zero := append([]string{"--part-interval=0"}, common...)
	ref := filepath.Join(t.TempDir(), "ref")
	refRep, err := run(listArgs(ref, zero), "", 0)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	// At least about 16 parts, so K advances during a run (part records,
	// retirements by publication, the part-recorded arm).
	if b := refRep.Output.Bytes; b < 16*2<<20 {
		partSize = fmt.Sprintf("%dKiB", max(b/16>>10, 16))
		os.RemoveAll(ref)
		if refRep, err = run(listArgs(ref, zero), "", 0); err != nil {
			t.Fatalf("reference: %v", err)
		}
	}
	rows := refRep.Output.RowsCommitted
	// The independent oracle: a service that omits keys would otherwise
	// be its own reference.
	limit, _ := strconv.ParseInt(os.Getenv("SWATH_CAMPAIGN_MAX_KEYS"), 10, 64)
	sameKeys(t, ref, fx.keys, limit)
	slow := frontierEnd(t, ref, rows/10)
	px.slow.Store(&slow)
	coldDir := filepath.Join(t.TempDir(), "cold")
	cold, err := run(listArgs(coldDir, zero), "", 0)
	if err != nil {
		t.Fatalf("cold: %v", err)
	}
	sameParts(t, "cold", ref, coldDir)
	coldReq := cold.value("requests")
	d := cold.Resources.Spill.Durable
	if d == nil || d.SealedFiles == 0 {
		t.Fatalf("the cold run sealed no certificate: %+v", d)
	}
	t.Logf("%s: %d rows, parts of %s, cold %d requests, %d certificates sealed", fx.name, rows, partSize, coldReq, d.SealedFiles)
	results := os.Getenv("SWATH_CAMPAIGN_RESULTS")
	for _, mode := range []struct {
		name    string
		runtime []string
	}{{"interval-0", zero}, {"interval-" + campaignInterval, append([]string{"--part-interval", campaignInterval}, common...)}} {
		for _, arm := range campaignArms {
			t.Run(mode.name+"/"+arm.name, func(t *testing.T) {
				var line string
				t.Cleanup(func() {
					status := "pass"
					if t.Failed() {
						status = "FAIL"
					}
					appendResult(t, results, fmt.Sprintf("%s\t%s\t%s\t%s\t%s", fx.name, mode.name, arm.name, status, line))
				})
				out := filepath.Join(t.TempDir(), "ds")
				if _, err := run(listArgs(out, mode.runtime), arm.kill, int64(arm.keys*float64(rows))); err != nil {
					t.Fatalf("kill at %s%v: %v", arm.kill, arm.keys, err)
				}
				_, offs, _, _ := ledgerPrefix(t, out)
				ents, _ := os.ReadDir(filepath.Join(out, order.DurableDir))
				t.Logf("killed: %d ledger frames, %d durable entries, K %q", len(offs), len(ents), lastKey(t, out))
				if arm.damage != nil {
					arm.damage(t, out)
				}
				resumeArgs := append(append([]string{"resume"}, mode.runtime...), "--report-format", "json", out)
				// logged runs a resume with the request log on and checks
				// it: no data request starts inside a certificate live
				// in the ledger's prefix when it starts.
				inside := 0
				logged := func(what, killAt string) (*campaignReport, []ledgerCert, string, []string) {
					certs, k := liveCerts(t, out), lastKey(t, out)
					px.mu.Lock()
					px.starts = nil
					px.mu.Unlock()
					px.logging.Store(true)
					r, err := run(resumeArgs, killAt, 0)
					px.logging.Store(false)
					if err != nil {
						t.Fatalf("%s: %v", what, err)
					}
					px.mu.Lock()
					starts := px.starts
					px.mu.Unlock()
					n, first := startsInside(starts, certs, k)
					if inside += n; n > 0 {
						t.Errorf("%s: %d data requests start inside live certificates, the first at %q (K %q)", what, n, first, k)
					}
					t.Logf("%s: %d live certificates, %d eligible (above K), %d data requests logged", what, len(certs), eligibleCerts(certs, k), len(starts))
					return r, certs, k, starts
				}
				if arm.resumeKill != "" {
					logged("the first resume, killed at "+arm.resumeKill, arm.resumeKill)
				}
				r, certs, k, starts := logged("the resume", "")
				if mode.runtime[0] == zero[0] {
					sameParts(t, arm.name, ref, out)
				} else {
					sameRowsStreamed(t, arm.name, ref, out)
				}
				// Reconciliation.
				o := r.Output
				var replayed, accepted, rejected int64
				if d := r.Resources.Spill.Durable; d != nil && d.Resume != nil {
					replayed, accepted = d.Resume.ReplayedRows, d.Resume.Accepted
					for _, n := range d.Resume.Rejected {
						rejected += n
					}
				}
				if o.RowsCommitted != rows || o.RowsRetained+o.RowsListed != rows || r.value("objects")-o.RowsFiltered+replayed != o.RowsListed {
					t.Errorf("reconciliation: committed %d (want %d), retained %d + listed %d, objects %d - filtered %d + replayed %d",
						o.RowsCommitted, rows, o.RowsRetained, o.RowsListed, r.value("objects"), o.RowsFiltered, replayed)
				}
				// Every live certificate is replayed unless rejected, and
				// these schedules damage none.
				if rejected != 0 {
					t.Errorf("%d certificates rejected", rejected)
				}
				req := r.value("requests")
				if req > 0 && len(starts) == 0 {
					t.Errorf("the resume made %d requests and the log holds no data request: the reuse check saw nothing", req)
				}
				elig := eligibleCerts(certs, k)
				if arm.reuse && (elig == 0 || accepted == 0 || replayed == 0) {
					t.Errorf("a reuse arm without reuse: %d eligible certificates, %d accepted, %d rows replayed", elig, accepted, replayed)
				}
				line = fmt.Sprintf("%d\t%d\t%d\t%d\t%d\t%.3f\t%d\t%d\t%d", len(certs), elig, accepted, replayed, req, float64(req)/float64(coldReq), r.value("reuse.boundary_overfetch_keys"), len(starts), inside)
				t.Logf("accepted %d, replayed %d rows, resume %d requests (%.3fx cold)", accepted, replayed, req, float64(req)/float64(coldReq))
			})
		}
	}
	interval := append([]string{"--part-interval", campaignInterval}, common...)
	t.Run("interval-"+campaignInterval+"/suspect-composition", func(t *testing.T) {
		status, line := "FAIL", ""
		defer func() {
			if !t.Failed() {
				status = "pass"
			}
			appendResult(t, results, fmt.Sprintf("%s\tinterval-%s\tsuspect-composition\t%s\t%s", fx.name, campaignInterval, status, line))
		}()
		line = suspectComposition(t, rn, ref, interval, func(out string) []string { return listArgsSized(out, "64MiB", interval) })
	})
}

// suspectComposition is contract §6's "time cuts on" row: a part closed by
// the interval that holds rows replayed from two certificates and rows
// listed live between them, claimed and then killed before its part
// record, must be reproduced byte for byte by the next resume. The crashed
// run seals certificates one ledger cycle each (SWATH_TEST_SERIAL_SEALS)
// and is killed at its sixth ledger sync, so five are durable; parts are
// 64 MiB, so the interval closes them. Each try resumes a copy of that
// output, killed at its n-th journal append (the part claimed, its record
// not yet written), until the claimed part has that composition; then the
// resume must reproduce it (its bytes in data/) and the dataset must be
// the reference's rows.
func suspectComposition(t *testing.T, rn *campaignRunner, ref string, runtime []string, list func(out string) []string) string {
	crashed := filepath.Join(t.TempDir(), "crashed")
	if _, err := rn.run(list(crashed), "spill-ledger-sync:6", 0, "SWATH_TEST_SERIAL_SEALS=1"); err != nil {
		t.Fatalf("the crashed run: %v", err)
	}
	certs, k := liveCerts(t, crashed), lastKey(t, crashed)
	if n := eligibleCerts(certs, k); n < 2 {
		t.Fatalf("%d certificates above K after the crash; the composition needs two", n)
	}
	resume := append(append([]string{"resume"}, runtime...), "--report-format", "json")
	for n := 1; n <= 8; n++ {
		try := filepath.Join(t.TempDir(), "try")
		if err := os.CopyFS(try, os.DirFS(crashed)); err != nil {
			t.Fatal(err)
		}
		at := fmt.Sprintf("journal-append:%d", n)
		if _, err := rn.run(append(slices.Clone(resume), try), at, 0); err != nil {
			t.Fatalf("no part with the composition before the resume ended (%s): %v", at, err)
		}
		// The part claimed at the kill: the ordinal after the last record.
		st, err := journal.Read(try)
		if err != nil {
			t.Fatal(err)
		}
		ord := len(st.Records)
		claimed := filepath.Join(try, dataset.Workspace, "parts", dataset.PartName(ord)+dataset.ClaimSuffix)
		if _, err := os.Stat(claimed); err != nil {
			t.Fatalf("%s: no claimed part %d: %v", at, ord, err)
		}
		replayed, gaps, ok := composition(t, claimed, certs, k)
		if !ok {
			t.Logf("%s: part %d draws on %d certificates and %d live rows between them: not the composition", at, ord, replayed, gaps)
			continue
		}
		want, err := os.ReadFile(claimed)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rn.run(append(slices.Clone(resume), try), "", 0); err != nil {
			t.Fatalf("the resume after %s: %v", at, err)
		}
		got, err := os.ReadFile(filepath.Join(try, "data", dataset.PartName(ord)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("the suspect part %d (%d bytes) is not reproduced byte for byte: %d bytes, %v", ord, len(want), len(got), err)
		}
		sameRowsStreamed(t, "suspect-composition", ref, try)
		t.Logf("%s: suspect part %d (rows from %d certificates, %d live rows between them) reproduced byte for byte", at, ord, replayed, gaps)
		return fmt.Sprintf("%s\tpart %d\t%d certificates\t%d live rows\t%d bytes", at, ord, replayed, gaps, len(want))
	}
	t.Fatal("no claimed part with rows from two certificates and live rows between them in 8 tries")
	return ""
}

// composition reads the claimed part at path and counts the certificates
// it draws rows from and its live rows (above K, in no certificate) lying
// between two of those certificates' rows; ok when both are present: at
// least two certificates and one live row between them.
func composition(t *testing.T, path string, certs []ledgerCert, k string) (replayed, gaps int, ok bool) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("the claimed part: %v", err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := pq.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("the claimed part: %v", err)
	}
	r := pq.NewReader(pf)
	defer r.Close()
	from := map[int]bool{}
	var live []string
	first, last := "", ""
	buf := make([]pq.Row, 1024)
	for {
		n, err := r.ReadRows(buf)
		for _, row := range buf[:n] {
			key := string(row[0].ByteArray())
			in := -1
			for i, c := range certs {
				if key > max(c.cs, k) && (c.open || key <= c.ce) {
					in = i
					break
				}
			}
			switch {
			case in >= 0:
				from[in] = true
				if first == "" {
					first = key
				}
				last = key
			case key > k:
				live = append(live, key)
			}
		}
		if err != nil {
			break
		}
	}
	for _, key := range live {
		if key > first && key < last {
			gaps++
		}
	}
	return len(from), gaps, len(from) >= 2 && gaps > 0
}

// startsInside counts the request starts inside a certificate's
// [max(cs, K), ce) (open: no end), certificates wholly at or below K
// excepted, and returns the first.
func startsInside(starts []string, certs []ledgerCert, k string) (n int, first string) {
	for _, s := range starts {
		for _, c := range certs {
			if (c.open || c.ce > k) && s >= max(c.cs, k) && (c.open || s < c.ce) {
				if n++; n == 1 {
					first = s
				}
			}
		}
	}
	return n, first
}

// eligibleCerts counts the certificates with keys above K: those a resume
// replays.
func eligibleCerts(certs []ledgerCert, k string) int {
	n := 0
	for _, c := range certs {
		if c.open || c.ce > k {
			n++
		}
	}
	return n
}

// appendResult appends line to the results file (none: nothing).
func appendResult(t *testing.T, path, line string) {
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, line)
}

// sameKeys is the independent oracle: the dataset in dir lists exactly
// the key file's first limit lines (all with limit 0), in order.
func sameKeys(t *testing.T, dir, path string, limit int64) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	rs := rowStream(t, dir)
	defer rs.close()
	var n int64
	for ; limit == 0 || n < limit; n++ {
		if !sc.Scan() {
			break
		}
		r, ok := rs.next()
		if !ok {
			t.Fatalf("the dataset ends at row %d; the key file goes on with %q", n, sc.Text())
		}
		if got := string(r[0].ByteArray()); got != sc.Text() {
			t.Fatalf("row %d is %q; the key file has %q", n, got, sc.Text())
		}
	}
	if _, ok := rs.next(); ok {
		t.Fatalf("the dataset has more than the key file's %d keys", n)
	}
}

// frontierEnd is the last key of the first part reaching rows rows of the
// dataset in dir: the skewed fixture's slow frontier ends there.
func frontierEnd(t *testing.T, dir string, rows int64) string {
	t.Helper()
	m := readManifest(t, dir)
	var n int64
	for _, p := range m.Parts {
		n += p.Rows
		if n >= rows {
			return string(p.MaxKey)
		}
	}
	t.Fatalf("%s: fewer than %d rows", dir, rows)
	return ""
}

func readManifest(t *testing.T, dir string) dataset.Manifest {
	t.Helper()
	var m dataset.Manifest
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil || json.Unmarshal(b, &m) != nil {
		t.Fatalf("manifest of %s: %v", dir, err)
	}
	return m
}

// sameRowsStreamed compares two datasets row by row, every column, in
// manifest order, holding one batch of each at a time: parts closed by time
// differ between runs, their rows may not.
func sameRowsStreamed(t *testing.T, at, ref, out string) {
	t.Helper()
	a, b := rowStream(t, ref), rowStream(t, out)
	defer a.close()
	defer b.close()
	for n := 0; ; n++ {
		ra, oka := a.next()
		rb, okb := b.next()
		if !oka || !okb {
			if oka != okb {
				t.Fatalf("%s: row counts differ at row %d", at, n)
			}
			return
		}
		if !ra.Equal(rb) {
			t.Fatalf("%s: row %d differs", at, n)
		}
	}
}

type rowReader struct {
	t     *testing.T
	dir   string
	parts []dataset.ManifestPart
	f     *os.File
	r     *pq.Reader
	buf   []pq.Row
	i, n  int
}

func rowStream(t *testing.T, dir string) *rowReader {
	return &rowReader{t: t, dir: dir, parts: readManifest(t, dir).Parts, buf: make([]pq.Row, 1024)}
}

func (s *rowReader) next() (pq.Row, bool) {
	for s.i == s.n {
		if s.r != nil {
			n, err := s.r.ReadRows(s.buf)
			s.i, s.n = 0, n
			if n > 0 {
				break
			}
			if err != nil && !errors.Is(err, io.EOF) {
				s.t.Fatal(err)
			}
			s.close()
		}
		if len(s.parts) == 0 {
			return nil, false
		}
		p := s.parts[0]
		s.parts = s.parts[1:]
		f, err := os.Open(filepath.Join(s.dir, p.Path))
		if err != nil {
			s.t.Fatal(err)
		}
		pf, err := pq.OpenFile(f, p.Bytes)
		if err != nil {
			s.t.Fatal(err)
		}
		s.f, s.r = f, pq.NewReader(pf)
	}
	s.i++
	return s.buf[s.i-1], true
}

func (s *rowReader) close() {
	if s.r != nil {
		s.r.Close()
		s.f.Close()
		s.r, s.f = nil, nil
	}
}

// ledgerCert is a sealed certificate's interval (cs, ce].
type ledgerCert struct {
	cs, ce string
	open   bool
}

// ledgerPrefix is the ledger's durable prefix in out (contract §2: frames
// up to the first invalid one): each frame's offset, type and body.
func ledgerPrefix(t *testing.T, out string) (b []byte, offs []int, types []byte, bodies [][]byte) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(out, order.DurableDir, "ledger"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil, nil
	}
	if err != nil {
		t.Fatal(err)
	}
	tab := crc32.MakeTable(crc32.Castagnoli)
	for off := 0; off+9 <= len(b); {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		if n < 1 || off+8+n > len(b) {
			break
		}
		if crc32.Update(crc32.Checksum(b[off:off+4], tab), tab, b[off+8:off+8+n]) != binary.LittleEndian.Uint32(b[off+4:]) {
			break
		}
		offs, types, bodies = append(offs, off), append(types, b[off+8]), append(bodies, b[off+9:off+8+n])
		off += 8 + n
	}
	return b, offs, types, bodies
}

// liveCerts is the certificates sealed and not retired in out's ledger
// prefix: what a resume's recovery accepts when their files are intact.
func liveCerts(t *testing.T, out string) []ledgerCert {
	t.Helper()
	_, _, types, bodies := ledgerPrefix(t, out)
	live := map[string]ledgerCert{}
	str := func(b []byte) (string, []byte) {
		n, k := binary.Uvarint(b)
		if k <= 0 || uint64(len(b)-k) < n {
			t.Fatalf("malformed ledger record")
		}
		return string(b[k : k+int(n)]), b[k+int(n):]
	}
	for i, typ := range types {
		body := bodies[i]
		switch {
		case typ == 'S' && len(body) > 16:
			cs, rest := str(body[16:])
			ce, rest := str(rest)
			live[string(body[:16])] = ledgerCert{cs: cs, ce: ce, open: len(rest) > 0 && rest[0] == 1}
		case typ == 'X' && len(body) >= 16:
			delete(live, string(body[:16]))
		}
	}
	certs := make([]ledgerCert, 0, len(live))
	for _, c := range live {
		certs = append(certs, c)
	}
	return certs
}

// lastKey is K: the last key of the last part the journal in out records
// ("" with none).
func lastKey(t *testing.T, out string) string {
	t.Helper()
	st, err := journal.Read(out)
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	k := ""
	for _, r := range st.Records {
		if r.Type == "part" {
			k = string(r.Part.MaxKey)
		}
	}
	return k
}

// loseUnsealed is the power loss after a kill at a seal: every durable
// file without a record in the ledger (not yet synced) loses its last 32
// bytes.
func loseUnsealed(t *testing.T, out string) {
	t.Helper()
	_, _, types, bodies := ledgerPrefix(t, out)
	recorded := map[string]bool{}
	for i, typ := range types {
		if typ == 'S' && len(bodies[i]) >= 16 {
			recorded[fmt.Sprintf("%x.spill", bodies[i][:16])] = true
		}
	}
	ents, _ := os.ReadDir(filepath.Join(out, order.DurableDir))
	cut := 0
	for _, e := range ents {
		p := filepath.Join(out, order.DurableDir, e.Name())
		if fi, err := os.Stat(p); err == nil && strings.HasSuffix(e.Name(), ".spill") && !recorded[e.Name()] && fi.Size() > 64 {
			os.Truncate(p, fi.Size()-32)
			cut++
		}
	}
	if cut == 0 {
		t.Fatal("no unsealed durable file to cut")
	}
}

// tearLedgerTail is the power loss after a kill at a ledger sync: the
// unsynced append is torn inside its last frame.
func tearLedgerTail(t *testing.T, out string) {
	t.Helper()
	b, offs, _, _ := ledgerPrefix(t, out)
	if len(offs) < 3 {
		t.Fatalf("a ledger of %d frames: nothing to tear after the header", len(offs))
	}
	last := offs[len(offs)-1]
	if err := os.Truncate(filepath.Join(out, order.DurableDir, "ledger"), int64(last+(len(b)-last)/2)); err != nil {
		t.Fatal(err)
	}
}

// TestCampaignRunnerFaults: the campaign accepts a kill only as the fault
// it asked for. A run its watchdog ends fails although a SIGKILL ended it
// (before, any SIGKILL passed: an unreached hook in a hung run counted as
// reached after the 20-minute watchdog); an unreached hook and a key
// threshold never crossed fail; a reached hook passes.
func TestCampaignRunnerFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	url := s3test.Serve(t, s3test.Handler(s3test.Backend(t, s3test.Keys(3000, 7))))
	args := func() []string {
		return []string{"list", "s3://bkt", "-endpoint", url, "-path-style", "-anonymous", "--part-interval=0", "-o", filepath.Join(t.TempDir(), "ds")}
	}
	var keys atomic.Int64
	rn := &campaignRunner{bin: bin, env: os.Environ(), keys: &keys, timeout: time.Minute}
	if _, err := rn.run(args(), "response-read:2", 0); err != nil {
		t.Fatalf("a reached hook: %v", err)
	}
	if _, err := rn.run(args(), "spill-replay-frame:1", 0); err == nil || !strings.Contains(err.Error(), "not reached") {
		t.Fatalf("an unreached hook on a completing run: %v", err)
	}
	if _, err := rn.run(args(), "", 1<<40); err == nil || !strings.Contains(err.Error(), "not killed at") {
		t.Fatalf("a key threshold never crossed: %v", err)
	}
	hung := &campaignRunner{bin: bin, env: append(os.Environ(), "SWATH_TEST_HANG_AT=response-read:1"), keys: &keys, timeout: 3 * time.Second}
	if _, err := hung.run(args(), "spill-replay-frame:1", 0); err == nil || !strings.Contains(err.Error(), "watchdog") {
		t.Fatalf("a hung run the watchdog killed: %v", err)
	}
}
