// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/internal/source/s3/client"
)

// hotVersions is a versioned bucket with hot keys for durable spill: 4,000
// keys, every 37th with 40 versions (a group spanning several 7-entry
// pages, so page boundaries fall inside groups), the rest 1 to 3; a slow
// frontier (fresh requests below the 10 % key wait), so the ranges ahead
// spill and seal certificates, and a gate (versionsGate) on the frontier's
// second page. It logs each data request (no delimiter)
// while logging is on: its key-marker and whether it continues a page
// (a version-id-marker). A fresh request's key-marker is its exclusive
// start, as start-after is for ListObjectsV2; a continuation lists from
// the rest of its key-marker's group.
type hotVersions struct {
	rows    []client.Version
	slow    string
	gate    atomic.Pointer[versionsGate] // the run in progress's (nil: no hold)
	late    atomic.Bool                  // a gate's signal did not come within its cap
	logging atomic.Bool
	mu      sync.Mutex
	starts  []versionsReq
}

// versionsGate holds the frontier's second page of one run (in group mode
// each page starts fresh after the last complete key) until the ranges
// ahead have sealed eight certificates (order's "sealed" events): K stays
// at the bottom while they seal, so the kills find certificates live above
// it. A run that fails while held (a kill) ends the request with its
// context. Resumes are not held (hotVersionsConfig). No release within two minutes is a failure of the fixture
// (hotVersions.late), not a release by time.
type versionsGate struct {
	release chan struct{}
	once    sync.Once
	seen    atomic.Int64 // fresh requests from the scope's start this run
	sealed  atomic.Int64
	// last, if set, holds the frontier instead until every other range is
	// done (order's "one-range-open") and a certificate is sealed: listing
	// then ends inside the frontier's range, the sealed files ahead of it.
	last     bool
	lastSeen atomic.Bool
	// hold, if set, never releases the frontier: the run is killed while
	// it holds, every certificate sealed above K.
	hold bool
}

// event is the order stage's event hook (order.SetDurableEvents) for the
// run in progress.
func (p *hotVersions) event(e string) {
	g := p.gate.Load()
	if g == nil {
		return
	}
	switch e {
	case "sealed":
		g.sealed.Add(1)
	case "one-range-open":
		g.lastSeen.Store(true)
	default:
		return
	}
	if g.hold {
		return
	}
	if g.last && g.lastSeen.Load() && g.sealed.Load() > 0 || !g.last && g.sealed.Load() >= 8 {
		g.once.Do(func() { close(g.release) })
	}
}

// versionsReq is a logged data request: its key-marker, and whether it
// carried a version-id-marker (a page continuation).
type versionsReq struct {
	marker string
	cont   bool
}

func newHotVersions(t *testing.T) *hotVersions { return newHotVersionsN(t, 4000) }

// newHotVersionsN is newHotVersions with keys keys. Past 10,000 the keys
// take five digits, so the rows are put in key order (a key's versions
// keep theirs) and their version IDs, the rows' indexes, renumbered.
func newHotVersionsN(t *testing.T, keys int) *hotVersions {
	p := &hotVersions{}
	for i := range keys {
		n := 1 + i%3
		if i%37 == 5 {
			n = 40
		}
		key := fmt.Sprintf("k%04d", i)
		for j := range n {
			p.rows = append(p.rows, client.Version{Object: client.Object{Key: key, Size: int64(j), Mtime: 1, ETag: "e"},
				VersionID: strconv.Itoa(len(p.rows)), IsLatest: j == 0, DeleteMarker: j%9 == 8})
		}
	}
	sort.SliceStable(p.rows, func(i, j int) bool { return p.rows[i].Key < p.rows[j].Key })
	for i := range p.rows {
		p.rows[i].VersionID = strconv.Itoa(i)
	}
	p.slow = "k0400"
	t.Cleanup(func() {
		if p.late.Load() {
			t.Error("the frontier's hold was not released by the run within two minutes")
		}
	})
	return p
}

func (p *hotVersions) Route(string) string { return "https://versions.invalid/b" }
func (p *hotVersions) ListObjectsV2(context.Context, *client.ListInput) (*client.Page, error) {
	return nil, errors.New("hotVersions lists versions only")
}

func (p *hotVersions) ListObjectVersions(ctx context.Context, in *client.VersionsInput) (*client.VersionPage, error) {
	if g := p.gate.Load(); g != nil && in.Delimiter == "" && in.VersionIDMarker == "" && in.KeyMarker != "" && in.KeyMarker < "k0010" && g.seen.Add(1) == 1 {
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(2 * time.Minute):
			p.late.Store(true)
		}
	}
	if in.Delimiter == "" && p.logging.Load() {
		p.mu.Lock()
		p.starts = append(p.starts, versionsReq{in.KeyMarker, in.VersionIDMarker != ""})
		p.mu.Unlock()
	}
	if in.Delimiter == "" && in.VersionIDMarker == "" {
		if in.KeyMarker < p.slow {
			time.Sleep(20 * time.Millisecond)
		}
	}
	time.Sleep(200 * time.Microsecond)
	i := sort.Search(len(p.rows), func(i int) bool { return p.rows[i].Key > in.KeyMarker })
	if in.VersionIDMarker != "" {
		n, _ := strconv.Atoi(in.VersionIDMarker)
		i = n + 1
	}
	end := min(len(p.rows), i+7)
	out := &client.VersionPage{Versions: append([]client.Version(nil), p.rows[i:end]...)}
	if end < len(p.rows) {
		out.IsTruncated = true
		out.NextKeyMarker = p.rows[end-1].Key
		out.NextVersionIDMarker = p.rows[end-1].VersionID
	}
	return out, nil
}

func hotVersionsConfig(api *hotVersions, out string, resume bool) Config {
	// Only a fresh run is held: a resume replays the certificates and may
	// seal fewer than eight, so a gate would wait for a signal that never
	// comes (the 5 s hold this replaced passed it on time).
	api.gate.Store(nil)
	if !resume {
		api.gate.Store(&versionsGate{release: make(chan struct{})})
	}
	return Config{Source: SourceConfig{URI: "s3://b", Options: &s3.Options{Client: api, AllVersions: true, Policy: keyrange.Policy{PageSize: 7}}},
		Output: OutputConfig{Dir: out, Resume: resume, NoJournal: datasettest.NoJournal()},
		Limits: Limits{Workers: 16, PartSize: 2 << 10, RowBudget: MinRowBudget, Spill: SpillLimits{Max: 64 << 20}, PartInterval: -1}}
}

// versionCert is a sealed certificate's interval (cs, ce].
type versionCert struct {
	cs, ce string
	open   bool
}

// liveVersionCerts reads the certificates sealed and not retired in out's
// ledger prefix (frames up to the first invalid one).
func liveVersionCerts(t *testing.T, out string) []versionCert {
	t.Helper()
	certs, err := readVersionCerts(out)
	if err != nil {
		t.Fatal(err)
	}
	return certs
}

// readVersionCerts is liveVersionCerts without a test: for a crash-point
// hook, off the test's goroutine.
func readVersionCerts(out string) ([]versionCert, error) {
	b, err := os.ReadFile(filepath.Join(out, order.DurableDir, "ledger"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	tab := crc32.MakeTable(crc32.Castagnoli)
	str := func(b []byte) (string, []byte) {
		n, k := binary.Uvarint(b)
		return string(b[k : k+int(n)]), b[k+int(n):]
	}
	live := map[string]versionCert{}
	for off := 0; off+9 <= len(b); {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		if n < 1 || off+8+n > len(b) || crc32.Update(crc32.Checksum(b[off:off+4], tab), tab, b[off+8:off+8+n]) != binary.LittleEndian.Uint32(b[off+4:]) {
			break
		}
		typ, body := b[off+8], b[off+9:off+8+n]
		switch {
		case typ == 'S' && len(body) > 16:
			cs, rest := str(body[16:])
			ce, rest := str(rest)
			live[string(body[:16])] = versionCert{cs, ce, len(rest) > 0 && rest[0] == 1}
		case typ == 'X' && len(body) >= 16:
			delete(live, string(body[:16]))
		}
		off += 8 + n
	}
	var certs []versionCert
	for _, c := range live {
		certs = append(certs, c)
	}
	return certs, nil
}

// TestVersionsDurableSpill: an --all-versions run with hot keys (groups of
// 40 versions over 7-entry pages) spills and seals durable certificates;
// killed at the durable spill's steps (a seal, a ledger append, the first
// unlink) and mid recovery replay, the resume replays the certificates it
// accepts and lists the gaps, and the dataset is byte-identical to an
// uninterrupted run (the run reconciles its rows itself): no version is
// lost or repeated at a certificate's bounds. No fresh data request of a
// resume starts inside a live certificate's [max(cs, K), ce). The decision behind it: in group mode a page end that
// leaves a hot key's group open declares no fence (groups.fence_deferred),
// so certificates begin and end at complete keys, and a replay accepts a
// key repeated within its group.
func TestVersionsDurableSpill(t *testing.T) {
	datasettest.RequireJournal(t)
	api := newHotVersions(t)
	defer order.SetDurableEvents(api.event)()
	ref := filepath.Join(t.TempDir(), "ref")
	cold, err := Run(context.Background(), hotVersionsConfig(api, ref, false))
	if err != nil {
		t.Fatal(err)
	}
	if d := cold.Resources.Spill.Durable; d == nil || d.SealedFiles == 0 {
		t.Fatalf("no certificate sealed: %+v", cold.Resources.Spill)
	}
	// The hot keys' open groups deferred fences (the path the decision adds).
	if b, _ := json.Marshal(cold.Source); !regexp.MustCompile(`"groups\.fence_deferred":[1-9]`).Match(b) {
		t.Fatalf("no fence deferred for an open group: %s", b)
	}
	for _, kill := range []struct {
		point      string
		nth        int64
		resumeKill string
	}{
		{"spill-seal", 2, ""},
		{"spill-ledger-append", 2, ""},
		{"spill-unlink", 1, ""},
		{"spill-unlink", 1, "spill-replay-frame"},
		// A part claimed, its record not written: the resume reproduces
		// it pinned at its claimed row count from replayed and listed
		// rows, the same stream as the cold run's, so it neither cuts
		// inside a group nor refuses with ErrAmbiguousTail.
		{"journal-append", 12, ""},
		{"journal-append", 12, "spill-replay-frame"},
	} {
		name := kill.point
		if kill.resumeKill != "" {
			name += "+" + kill.resumeKill
		}
		t.Run(name, func(t *testing.T) {
			// The journal-append case's point: the claimed part after K
			// starts inside a live certificate, so the resume reproduces it
			// from replayed rows and listed ones together. Whether the
			// twelfth record's K is inside one is the schedule's: the range
			// there may have been drained without spilling (a third of
			// kills at the twelfth record on a shared box; six in a row on
			// CI). The run is therefore killed at the first append from the
			// twelfth on whose K is inside a live certificate; a run where
			// none is completes, and is run again on a fresh output, at
			// most six times: the case never passes on listed rows alone.
			var out string
			for attempt := 1; ; attempt++ {
				out = filepath.Join(t.TempDir(), "ds")
				if kill.point != "journal-append" {
					crashDurable(t, hotVersionsConfig(api, out, false), kill.point, kill.nth)
					break
				}
				if killInsideCert(t, hotVersionsConfig(api, out, false), out, kill.nth) && claimInsideCert(t, out) {
					break
				}
				if attempt == 6 {
					t.Fatalf("%d runs, none with a record's K inside a live certificate from the %dth record on", attempt, kill.nth)
				}
			}
			if kill.resumeKill != "" {
				crashDurable(t, hotVersionsConfig(api, out, true), kill.resumeKill, 2)
			}
			certs, k := liveVersionCerts(t, out), versionsK(t, out)
			api.mu.Lock()
			api.starts = nil
			api.mu.Unlock()
			api.logging.Store(true)
			sum, err := Run(context.Background(), hotVersionsConfig(api, out, true))
			api.logging.Store(false)
			if err != nil {
				t.Fatal(err)
			}
			sameDataset(t, name, ref, out)
			api.mu.Lock()
			for _, r := range api.starts {
				for _, c := range certs {
					lo := max(c.cs, k)
					if !c.open && c.ce <= k {
						continue // covered by K: nothing of it is replayed
					}
					// A fresh request returns keys above its marker; a
					// continuation also the rest of its marker's group.
					// Either is inside when it can return a key of
					// (max(cs, K), ce]. A continuation at cs or K finishes
					// a complete key below the replay.
					if in := !r.cont && r.marker >= lo && (c.open || r.marker < c.ce) ||
						r.cont && r.marker > lo && (c.open || r.marker <= c.ce); in {
						t.Errorf("a data request (key-marker %q, continuation %v) lists inside the certificate (%q, %q] (K %q)", r.marker, r.cont, c.cs, c.ce, k)
					}
				}
			}
			api.mu.Unlock()
			if d := sum.Resources.Spill.Durable; d != nil && d.Resume != nil {
				t.Logf("K %q, %d live certificates, accepted %d, replayed %d rows", k, len(certs), d.Resume.Accepted, d.Resume.ReplayedRows)
			}
		})
	}
}

// claimInsideCert reports whether out, killed at a journal append, has a
// claimed part to reproduce (failing if not) whose start, after K, lies
// inside a live certificate.
func claimInsideCert(t *testing.T, out string) bool {
	t.Helper()
	st, err := journal.Read(out)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range st.Records {
		if r.Type == "part" {
			n++
		}
	}
	if _, err := os.Stat(dataset.ClaimPath(out, n)); err != nil {
		t.Fatalf("no claimed part %d to reproduce: %v", n, err)
	}
	k := versionsK(t, out)
	for _, c := range liveVersionCerts(t, out) {
		if c.cs <= k && (c.open || k < c.ce) {
			t.Logf("the claimed part after K %q starts inside the certificate (%q, %q]: replayed", k, c.cs, c.ce)
			return true
		}
	}
	t.Logf("K %q is inside no live certificate: killed again", k)
	return false
}

// killInsideCert runs cfg, writing to out, and kills it at the first
// journal append from the nth on whose K (the last recorded part's last
// key) lies inside a live certificate. It reports false when the run
// completed without one.
func killInsideCert(t *testing.T, cfg Config, out string, nth int64) bool {
	t.Helper()
	var seen atomic.Int64
	inside := func() bool {
		st, err := journal.Read(out)
		if err != nil {
			return false
		}
		k := ""
		for _, r := range st.Records {
			if r.Type == "part" {
				k = string(r.Part.MaxKey)
			}
		}
		certs, err := readVersionCerts(out)
		if err != nil {
			return false
		}
		for _, c := range certs {
			if c.cs <= k && (c.open || k < c.ce) {
				return true
			}
		}
		return false
	}
	restore := faults.Set(gated(func(p string) error {
		if p == "journal-append" && seen.Add(1) >= nth && inside() {
			return faults.ErrCrash
		}
		return nil
	}))
	_, err := Run(context.Background(), cfg)
	restore()
	if err == nil {
		t.Logf("completed with no record's K inside a live certificate (%d appends): run again", seen.Load())
		return false
	}
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("killed inside a certificate: %v", err)
	}
	return true
}

// versionsK is the last key of the last part the journal in out records.
func versionsK(t *testing.T, out string) string {
	t.Helper()
	st, err := journal.Read(out)
	if err != nil {
		t.Fatal(err)
	}
	k := ""
	for _, r := range st.Records {
		if r.Type == "part" {
			k = string(r.Part.MaxKey)
		}
	}
	return k
}

// TestVersionsTailSeamsAtKeyChanges: the seam rule for the drain tail
// (docs/internals/ordering.md, "Repeated keys"). Behind a slow frontier
// the ranges ahead finish first and their rows are drained at the end, a
// tail through the router; parts are cut every 5 rows, so most cut points
// fall inside a hot key's 40 versions. Every cut must move to the next key
// change: each part's last key is below the next part's first, so no key
// spans two parts. (A row cap never freezes the router's size rule, so this
// tail is never segmented; TestVersionsSegmentsAtKeyChanges holds the
// segmented tail to the same rule.)
func TestVersionsTailSeamsAtKeyChanges(t *testing.T) {
	api := newHotVersions(t)
	out := filepath.Join(t.TempDir(), "ds")
	cfg := hotVersionsConfig(api, out, false)
	cfg.Limits.Spill = SpillLimits{Off: true}
	cfg.Limits.PartMaxRows = 5
	cfg.Limits.RowBudget = 64 << 20 // the tail: every range ahead buffered, drained at the end
	api.gate.Store(nil)             // no durable spill to wait for: the slow region alone holds the frontier back
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	m := manifestOf(t, out)
	if len(m.Parts) < 100 {
		t.Fatalf("%d parts: the cuts are not exercised", len(m.Parts))
	}
	inside := 0
	for i := 1; i < len(m.Parts); i++ {
		a, b := string(m.Parts[i-1].MaxKey), string(m.Parts[i].MinKey)
		if a >= b {
			t.Fatalf("part %d ends at %q and part %d starts at %q: a key spans a seam", i-1, a, i, b)
		}
		if m.Parts[i-1].Rows > 5 {
			inside++ // a cut deferred past a group
		}
	}
	if inside == 0 {
		t.Fatal("no cut was deferred to a key change: no group straddled a 5-row boundary")
	}
}

// TestVersionsSegmentsAtKeyChanges: the seam rule on the segment-parallel
// tail (order's segments.go). Behind the slow region every range ahead is
// listed and held in memory, so when listing ends the rest of the stream
// is cut into segments. Parts are cut by size (a row cap never freezes the
// router's size rule, so it would never plan segments), and planned
// boundaries fall inside hot keys' 40 versions: the planner moves each to
// the next key change, as the router defers its own cut
// (drain.segments.deferred_rows), no key spans two parts, and the dataset
// is byte-identical to the serial tail's (one encoder: no segments), its
// cut file too (the segments' parts' cuts joined in ordinal order).
// order's TestSegmentsCutAtKeyChanges covers a spilled tail.
func TestVersionsSegmentsAtKeyChanges(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4)) // spare processors for the segment workers
	api := newHotVersions(t)
	run := func(name string, encoders int) (string, Report) {
		t.Helper()
		out := filepath.Join(t.TempDir(), name)
		cfg := hotVersionsConfig(api, out, false)
		cfg.Limits.Encoders = encoders
		cfg.Limits.Spill = SpillLimits{Off: true}
		cfg.Limits.RowBudget = 64 << 20 // every range ahead buffered behind the slow region: the rest of the stream when listing ends
		api.gate.Store(nil)             // the slow region alone holds the frontier back
		sum, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		m := manifestOf(t, out)
		for i := 1; i < len(m.Parts); i++ {
			if a, b := string(m.Parts[i-1].MaxKey), string(m.Parts[i].MinKey); a >= b {
				t.Fatalf("%s: part %d ends at %q and part %d starts at %q: a key spans a seam", name, i-1, a, i, b)
			}
		}
		if d := sum.Resources.Drain; d == nil || d.Segments == nil {
			t.Fatalf("%s: no segments in the report", name)
		}
		return out, sum
	}
	ref, serial := run("serial", 1)
	if sg := serial.Resources.Drain.Segments; sg.Reason == order.SegmentEngaged {
		t.Fatalf("serial: segments %+v", *sg)
	}
	out, sum := run("segments", 4)
	sg := sum.Resources.Drain.Segments
	if sg.Reason != order.SegmentEngaged || sg.Segments < 3 || sg.DeferredRows == 0 {
		t.Fatalf("spill %+v segments %+v: want engaged, boundaries deferred to key changes", *sum.Resources.Spill, *sg)
	}
	t.Logf("segments: %d of %d rows, %d deferred", sg.Segments, sg.Rows, sg.DeferredRows)
	requireCuts(t, "segments", out)
	sameDataset(t, "segments", ref, out)
	// The segments' rows are classified and their keys counted as the
	// serial drain's are.
	a, b := serial.Output, sum.Output
	if *a.Versions != *b.Versions || *a.LiveObjects != *b.LiveObjects || *a.DistinctKeys != *b.DistinctKeys || *a.DeleteMarkers != *b.DeleteMarkers || *b.DistinctKeys != 4000 {
		t.Fatalf("versions %d/%d live %d/%d distinct %d/%d markers %d/%d (serial/segments)",
			*a.Versions, *b.Versions, *a.LiveObjects, *b.LiveObjects, *a.DistinctKeys, *b.DistinctKeys, *a.DeleteMarkers, *b.DeleteMarkers)
	}
}

// TestVersionsSegmentsReadDurableFiles: the segment-parallel tail over
// durable spill files under a versions schema. 30,000 keys with hot
// groups, journaled, the frontier held until every other range is done
// and a certificate sealed (versionsGate.last): the ranges ahead spill and
// seal behind it, listing ends inside the frontier's range, and the tail
// segments with workers reading the sealed files. The row budget is the
// one that leaves room for several workers (at the minimum budget the
// planner allows one). The dataset is byte-identical to a serial run held
// in memory, and no key spans a seam; so is a resume's whose segments
// replay its reused certificates, its cut file too (the parts written from
// certificates collected in publication order like any other).
func TestVersionsSegmentsReadDurableFiles(t *testing.T) {
	datasettest.RequireJournal(t)
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4)) // spare processors for the segment workers
	api := newHotVersionsN(t, 60000)
	defer order.SetDurableEvents(api.event)()
	config := func(out string) Config {
		cfg := hotVersionsConfig(api, out, false)
		cfg.Source.Options.(*s3.Options).Policy.PageSize = 100
		cfg.Limits.RowBudget = 10 << 20
		return cfg
	}
	ref := filepath.Join(t.TempDir(), "serial")
	cfg := config(ref)
	cfg.Limits.Encoders, cfg.Limits.Spill = 1, SpillLimits{Off: true}
	cfg.Limits.RowBudget = 64 << 20
	api.gate.Store(nil)
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "segments")
	cfg = config(out)
	cfg.Limits.Encoders = 4
	api.gate.Store(&versionsGate{release: make(chan struct{}), last: true})
	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	sg := sum.Resources.Drain.Segments
	if d := sum.Resources.Spill.Durable; d == nil || d.SealedFiles == 0 || sg == nil {
		t.Fatalf("no certificate sealed or no segments: spill %+v durable %+v drain %+v", *sum.Resources.Spill, sum.Resources.Spill.Durable, *sum.Resources.Drain.Segments)
	}
	if sg.Reason != order.SegmentEngaged || sg.Workers < 2 || sg.DurableFilesRead < 1 || sg.DeferredRows == 0 {
		t.Fatalf("spill %+v segments %+v: want engaged, two workers or more, durable files read, boundaries deferred to key changes", *sum.Resources.Spill, *sg)
	}
	t.Logf("segments: %d over %d workers (%d bytes each of %d free), %d durable files, %d boundaries in them, %d rows deferred; %d rows spilled",
		sg.Segments, sg.Workers, sg.PerWorkerBytes, sg.FreeBytes, sg.DurableFilesRead, sg.DurableBoundaries, sg.DeferredRows, sum.Resources.Spill.Rows)
	seams := func(name, out string) {
		t.Helper()
		sameDataset(t, name, ref, out)
		m := manifestOf(t, out)
		for i := 1; i < len(m.Parts); i++ {
			if a, b := string(m.Parts[i-1].MaxKey), string(m.Parts[i].MinKey); a >= b {
				t.Fatalf("%s: part %d ends at %q and part %d starts at %q: a key spans a seam", name, i-1, a, i, b)
			}
		}
	}
	requireCuts(t, "segments", out)
	seams("segments", out)
	// A resume that reuses certificates replays them in the segments
	// (#288). The run is killed at its third ledger sync with the frontier
	// held for good, certificates sealed above K; the resume's drainer
	// starts only once listing is done (order's "drain-start" held until
	// "listing-done"), so the segments replay every certificate, their
	// bounds complete keys: the dataset is the serial run's, no key spans
	// a seam.
	out = filepath.Join(t.TempDir(), "resumed")
	cfg = config(out)
	cfg.Limits.Encoders = 4
	api.gate.Store(&versionsGate{release: make(chan struct{}), hold: true})
	crashDurable(t, cfg, "spill-ledger-sync", 3)
	cfg = hotVersionsConfig(api, out, true)
	cfg.Source.Options.(*s3.Options).Policy.PageSize = 100
	cfg.Limits.RowBudget, cfg.Limits.Encoders = 10<<20, 4
	listed := make(chan struct{})
	var once sync.Once
	restore := order.SetDurableEvents(func(e string) {
		switch e {
		case "listing-done":
			once.Do(func() { close(listed) })
		case "drain-start":
			select {
			case <-listed:
			case <-time.After(time.Minute):
				t.Error("listing did not end with the drainer held")
			}
		}
	})
	sum, err = Run(context.Background(), cfg)
	restore()
	if err != nil {
		t.Fatal(err)
	}
	sg, d := sum.Resources.Drain.Segments, sum.Resources.Spill.Durable
	if d == nil || d.Resume == nil || d.Resume.Accepted == 0 || sg == nil || sg.Reason != order.SegmentEngaged || sg.Workers < 2 ||
		sg.CertificatesRead < 1 || int64(sg.CertificatesRead) != d.Resume.ReplayedCerts || sg.CertifiedRows != d.Resume.ReplayedRows {
		t.Fatalf("resume: durable %+v segments %+v: want engaged, every certificate replayed by the segments", d, sg)
	}
	t.Logf("resume: %d certificates accepted, %d replayed by the segments (%d rows, %d skipped), %d segments over %d workers, %d rows deferred",
		d.Resume.Accepted, sg.CertificatesRead, sg.CertifiedRows, d.Resume.ReplaySkippedRows, sg.Segments, sg.Workers, sg.DeferredRows)
	requireCuts(t, "resume", out)
	seams("resume", out)
}
