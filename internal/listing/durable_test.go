// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// reqLog records the listing requests a fake S3 endpoint served: their
// start-after (empty for a continuation) and count.
type reqLog struct {
	mu     sync.Mutex
	starts []string
	n      atomic.Int64
	on     atomic.Bool
}

// frontierKeys is the number of keys slowFrontier serves.
const frontierKeys = 30000

// slowFrontier serves the durable-spill fixture: 8,000 keys under nine
// folders, every request 1 ms, and a request whose start lies below slow
// 25 ms more, so the first tiles are a slow frontier and the rest race
// ahead of it and spill under a small budget.
func slowFrontier(t *testing.T, slow string) (string, *s3mem.Backend, *reqLog) {
	t.Helper()
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	b := s3test.Backend(t, gcstest.Keys(frontierKeys), s3mem.WithTimeSource(&step{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}))
	log := &reqLog{}
	h := s3test.Handler(b)
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if _, ok := q["list-type"]; ok && q.Get("delimiter") == "" {
			start := q.Get("start-after")
			if log.on.Load() {
				log.n.Add(1)
				log.mu.Lock()
				log.starts = append(log.starts, start)
				log.mu.Unlock()
			}
			if h := frontierHold.Load(); h != nil && q.Get("continuation-token") == "" && h.holds(start) {
				// The held request waits for the hold's release, or its
				// run's cancellation. A keyed hold never expires (its test's
				// barriers and deadline bound it); the other expires after
				// 20 s, marked late.
				var expire <-chan time.Time
				if h.engaged != nil {
					close(h.engaged)
				} else {
					expire = time.After(20 * time.Second)
				}
				select {
				case <-h.release:
				case <-r.Context().Done():
				case <-expire:
					h.late.Store(true)
				}
			}
			if q.Get("continuation-token") == "" && start < slow {
				time.Sleep(50 * time.Millisecond)
			}
		}
		time.Sleep(time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	return url, b, log
}

// sealGate makes the durable fixture's ledger history deterministic for
// one run. Without it seals within one 100 ms flusher window coalesce into
// one ledger append, so a 4th-occurrence ledger arm (spill-ledger-dirsync#4,
// -append#4, -sync#4) depended on how the seals fell in time (Sol's second
// review). With it each seal waits at its crash point until every earlier
// seal's record has been taken by a cycle, so every seal is a ledger
// append of its own: four seals, four appends.
type sealGate struct {
	mu       sync.Mutex   // serializes seals
	seals    int64        // seals past their crash point (mu)
	cycles   atomic.Int64 // flusher cycles that appended records
	stop     atomic.Bool  // the run crashed: nothing more to wait for
	timedOut atomic.Bool
}

// runGate is the gate of the run in progress (nil: none).
var runGate atomic.Pointer[sealGate]

// gateSeals installs a gate for the next run. The returned function
// removes it and fails the test if a seal's wait timed out.
func gateSeals(t *testing.T) func() {
	t.Helper()
	g := &sealGate{}
	restore := order.SetDurableEvents(func(e string) {
		if e == "cycle-records" {
			g.cycles.Add(1)
		}
	})
	runGate.Store(g)
	return func() {
		runGate.Store(nil)
		restore()
		if g.timedOut.Load() {
			t.Errorf("a seal waited 10 s for the previous seal's append (%d cycles, %d seals)", g.cycles.Load(), g.seals)
		}
	}
}

// at is the gate's crash-point hook: a seal waits until every earlier
// seal's record has been appended (or the run crashed).
func (g *sealGate) at(p string) {
	if p != "spill-seal" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for deadline := time.Now().Add(10 * time.Second); g.cycles.Load() < g.seals && !g.stop.Load(); {
		if time.Now().After(deadline) {
			g.timedOut.Store(true)
			break
		}
		time.Sleep(time.Millisecond)
	}
	g.seals++
}

// hold holds the frontier's first request until release is closed: only
// that request, so the ranges ahead (seed tiles, children split off it)
// keep listing, spilling and sealing.
type hold struct {
	release chan struct{}
	after   string        // keyed: the request held is the first to start after it
	engaged chan struct{} // keyed: closed once that request is held
	seen    atomic.Int64
	late    atomic.Bool
}

// keyedHold holds the first request starting after after.
func keyedHold(after string) *hold {
	return &hold{release: make(chan struct{}), after: after, engaged: make(chan struct{})}
}

// holds reports whether the request starting after start is the one held:
// the first after h.after when keyed, else the second request from the
// start of the bucket.
func (h *hold) holds(start string) bool {
	if h.after != "" {
		return start == h.after && h.seen.Add(1) == 1
	}
	return start == "" && h.seen.Add(1) == 2
}

// frontierHold is the hold of the run in progress (nil: none).
var frontierHold atomic.Pointer[hold]

// gated is a crash-point hook that consults the run's gate first; crash
// marks the gate stopped when it returns an injected crash.
func gated(crash func(p string) error) func(string) error {
	return func(p string) error {
		g := runGate.Load()
		if g != nil {
			g.at(p)
		}
		err := crash(p)
		if err != nil && g != nil {
			g.stop.Store(true)
		}
		return err
	}
}

// durableConfig is crashConfig with spill on, the part interval off.
func durableConfig(url, out string, resume bool) Config {
	cfg := crashConfig(url, out, resume)
	cfg.Limits.Spill = SpillLimits{Max: 64 << 20}
	cfg.Limits.Workers = 32 // many ranges ahead of the slow frontier, each spilling
	return cfg
}

// TestDurableSpillResume: a run whose frontier is slow spills the ranges
// ahead of it into durable files and seals their certificates; killed
// at its first retirement's unlink, the resume replays the certificates
// that K does not cover instead of listing them, lists only the gaps (no
// row is both listed and replayed), and the dataset is byte-identical to
// an uninterrupted run.
func TestDurableSpillResume(t *testing.T) {
	url, _, log := slowFrontier(t, "d06/")
	ref := filepath.Join(t.TempDir(), "ref")
	cold, err := Run(context.Background(), durableConfig(url, ref, false))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cold durable: %+v", cold.Resources.Spill.Durable)
	if d := cold.Resources.Spill.Durable; d == nil || d.SealedFiles == 0 {
		t.Fatalf("no certificate sealed: %+v", cold.Resources.Spill)
	} else if d.Slots != 1024 || d.HistoryCap != 40_000 || d.LimitBytes != 64<<20 || d.LimitedBy != "spill_cap" {
		t.Fatalf("limits of a 64 MiB spill cap: %d slots, H %d, from %d bytes (%s)", d.Slots, d.HistoryCap, d.LimitBytes, d.LimitedBy)
	}
	if _, err := os.Stat(filepath.Join(ref, order.DurableDir)); !os.IsNotExist(err) {
		t.Fatalf("the committed run left its durable spill: %v", err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	var seen atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "spill-unlink" && seen.Add(1) == 1 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err = Run(context.Background(), durableConfig(url, out, false))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	crashedFiles := len(durableFiles(t, out))
	t.Logf("%d durable files after the crash", crashedFiles)
	log.on.Store(true)
	sum, err := Run(context.Background(), durableConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "resume", ref, out)
	d := sum.Resources.Spill.Durable
	if d == nil || d.Resume == nil || d.Resume.Accepted == 0 || d.Resume.ReplayedRows == 0 {
		t.Fatalf("nothing reused: %+v %+v; files after the crash: %d", d, d.Resume, crashedFiles)
	}
	t.Logf("cold: %d sealed; resume: accepted %d, replayed %d rows, skipped %d, gaps %d, %d list requests (rows emitted %d, fetched %d, retained %d)",
		cold.Resources.Spill.Durable.SealedFiles, d.Resume.Accepted, d.Resume.ReplayedRows, d.Resume.ReplaySkippedRows, d.Resume.Gaps, log.n.Load(), sum.Output.RowsEmitted, sum.Output.RowsFetched, sum.Output.RowsRetained)
	// Every row handed on was listed or replayed, none both. Listed rows
	// are not required: a drain that certified every range ahead before
	// the kill leaves only empty gaps (ubuntu-24.04-arm CI).
	if o := sum.Output; o.RowsFetched+d.Resume.ReplayedRows != o.RowsEmitted {
		t.Fatalf("listed %d rows and replayed %d, handed on %d", o.RowsFetched, d.Resume.ReplayedRows, o.RowsEmitted)
	}
}

// crashDurable runs cfg until the crash point's nth occurrence (an
// injected crash, a kill's state) and checks it happened.
func crashDurable(t *testing.T, cfg Config, point string, nth int64) {
	t.Helper()
	var seen atomic.Int64
	restore := faults.Set(gated(func(p string) error {
		if p == point && seen.Add(1) == nth {
			return faults.ErrCrash
		}
		return nil
	}))
	_, err := Run(context.Background(), cfg)
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash at %s#%d not injected (reached %d): %v", point, nth, seen.Load(), err)
	}
}

// durableFixture is the slow-frontier bucket, its uninterrupted dataset,
// and a crashed run's output.
func durableFixture(t *testing.T) (url string, b *s3mem.Backend, ref, out string) {
	t.Helper()
	url, b, _ = slowFrontier(t, "d06/")
	ref = filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), durableConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out = filepath.Join(t.TempDir(), "ds")
	// Killed at the first retirement's unlink: its cycle appended every
	// seal queued before it, so certificates K does not cover are on disk.
	crashDurable(t, durableConfig(url, out, false), "spill-unlink", 1)
	return url, b, ref, out
}

// durableFiles is the durable spill's files in out, by name.
func durableFiles(t *testing.T, out string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(out, order.DurableDir))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".spill") {
			names = append(names, e.Name())
		}
	}
	return names
}

// ledgerFrames is the ledger's frames in out: each one's offset, type and,
// for a record, its file ID.
func ledgerFrames(t *testing.T, out string) (b []byte, offs []int, types []byte, ids [][]byte) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(out, order.DurableDir, "ledger"))
	if err != nil {
		t.Fatal(err)
	}
	offs, types, ids = parseLedger(b)
	return b, offs, types, ids
}

// parseLedger is ledgerFrames on the ledger's bytes b.
func parseLedger(b []byte) (offs []int, types []byte, ids [][]byte) {
	for off := 0; off+9 <= len(b); {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		if off+8+n > len(b) || n < 1 {
			break
		}
		offs, types = append(offs, off), append(types, b[off+8])
		var id []byte
		if n >= 17 {
			id = b[off+9 : off+25]
		}
		ids = append(ids, id)
		off += 8 + n
	}
	return offs, types, ids
}

// damagedSnapshot is a crashed run's output, damaged as Sol 1 schedules
// it: a recorded file missing (recovery retires it: its append, sync,
// unlink and directory sync are reached), an interior ledger record lost
// with the ones after it intact (persisted out of order: the truncation is
// reached), and an unrecorded file present (an orphan).
//
// The missing file's certificate must be live in the surviving prefix: a
// seal with no retirement after it. The fixture is killed at the first
// unlink, after that cycle appended its retirement record, so the first
// sealed file still on disk can be one whose retirement is already in the
// ledger; removing it then retires nothing, and recovery reached its
// retirement steps only when K happened to cover another certificate (the
// flake: 2 runs in 8 reached none, crashmatrix-chase.log).
func damagedSnapshot(t *testing.T, out string) {
	t.Helper()
	b, offs, types, ids := ledgerFrames(t, out)
	victim, lost := -1, -1
	retired := map[string]bool{}
	for i := range offs {
		if types[i] == 'X' && ids[i] != nil {
			retired[string(ids[i])] = true
		}
	}
	for i := 1; i < len(offs)-2; i++ { // before the lost record
		if types[i] != 'S' || retired[string(ids[i])] {
			continue
		}
		name := hex.EncodeToString(ids[i]) + ".spill"
		if os.Remove(filepath.Join(out, order.DurableDir, name)) == nil {
			victim = i
			break
		}
	}
	if victim > 0 && victim < len(offs)-2 {
		lost = len(offs) - 2 // interior, the last frame intact; the records before it stand
	}
	if victim < 0 || lost < 0 {
		t.Fatalf("the fixture's ledger is too short for the schedule: %d frames, types %q", len(offs), types)
	}
	for k := offs[lost]; k < offs[lost+1]; k++ {
		b[k] = 0
	}
	os.WriteFile(filepath.Join(out, order.DurableDir, "ledger"), b, 0o644)
	os.WriteFile(filepath.Join(out, order.DurableDir, "0123456789abcdef0123456789abcdef.spill"), []byte("orphan"), 0o644)
}

// snapshotSeals is the seal records a snapshot's ledger holds when its
// frontier is released or its run killed (heldSnapshot).
const snapshotSeals = 8

// certSpan is a live seal record's certificate: (cs, ce], ce ⊤ when open.
type certSpan struct {
	id     []byte
	cs, ce string
	open   bool
}

// against is how K (nil: no part recorded) stands to c at a recovery:
// "covered" (retired), "straddled" (its replay skips the rows at or below
// K) or "above".
func (c certSpan) against(k *string) string {
	switch {
	case k == nil || c.cs >= *k:
		return "above"
	case !c.open && c.ce <= *k:
		return "covered"
	}
	return "straddled"
}

// ledgerSeals is the live seal records of the ledger's bytes b, in ledger
// order (a retirement record removes its seal).
func ledgerSeals(b []byte) []certSpan {
	offs, types, ids := parseLedger(b)
	var seals []certSpan
	for i, off := range offs {
		switch types[i] {
		case 'S':
			d := schema.NewDecoder(b[off+9+16 : off+8+int(binary.LittleEndian.Uint32(b[off:]))])
			seals = append(seals, certSpan{id: ids[i], cs: d.Str(), ce: d.Str(), open: d.Byte() == 1})
		case 'X':
			seals = slices.DeleteFunc(seals, func(c certSpan) bool { return bytes.Equal(c.id, ids[i]) })
		}
	}
	return seals
}

// recordedK is the K of out's part journal (nil: no part recorded).
func recordedK(out string) (*string, error) {
	st, err := journal.Read(out)
	if err != nil || len(st.Records) == 0 {
		return nil, err
	}
	k := string(st.Records[len(st.Records)-1].Part.MaxKey)
	return &k, nil
}

// heldSnapshot is a durable-spill run's output built by barriers. Its
// frontier is held (the request continuing after the bucket's first page,
// 1,000 keys, waits), so no later row is handed on and K stays below every
// certificate, until a ledger sync finds snapshotSeals seal records or
// more with the hold engaged. Without advance the run is killed at that
// sync, the frontier still held: every ledger record is a live seal above
// K. With advance the ledger is frozen there (every later flusher cycle is
// killed at its directory sync, before it writes a record) and the
// frontier released; the run is killed at the first recorded part whose K
// covers a certificate damagedSnapshot keeps and lies inside another, with
// three kept certificates or more not covered. A covered certificate then
// has no retirement record: recovery retires it as covered, in one batch
// with the missing one.
func heldSnapshot(t *testing.T, url string, advance bool) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "ds")
	keys := slices.Sorted(slices.Values(gcstest.Keys(frontierKeys)))
	h := keyedHold(keys[999])
	release := sync.OnceFunc(func() { close(h.release) })
	frontierHold.Store(h)
	var (
		lifted, killed atomic.Bool
		kept           []certSpan // advance: the certificates damagedSnapshot keeps
		dead           = make(chan struct{})
		kill           = sync.OnceFunc(func() { close(dead) })
		why            atomic.Value // a barrier not met
		seals          atomic.Int64
	)
	restore := faults.Set(func(p string) error {
		switch {
		case p == "spill-ledger-sync" && !lifted.Load() && !killed.Load():
			b, _ := os.ReadFile(filepath.Join(out, order.DurableDir, "ledger")) // the cycle's append written, not yet synced
			s := ledgerSeals(b)
			if seals.Store(int64(len(s))); len(s) < snapshotSeals {
				return nil
			}
			select {
			case <-h.engaged:
			case <-time.After(20 * time.Second):
				why.Store(fmt.Sprintf("%d seal records, the frontier hold not engaged after 20 s", len(s)))
				killed.Store(true)
				return faults.ErrCrash
			}
			if !advance {
				killed.Store(true)
				return faults.ErrCrash // the frontier released only once Run returned
			}
			// Only seals so far (nothing retired with the frontier held):
			// damagedSnapshot removes the first one's file and loses the
			// last two records.
			kept = s[1 : len(s)-2]
			lifted.Store(true)
			release()
		case p == "spill-ledger-dirsync" && lifted.Load():
			select {
			case <-dead:
			case <-time.After(60 * time.Second):
				why.Store("no recorded part's K covered a kept certificate and straddled another within 60 s")
			}
			return faults.ErrCrash
		case p == "recorded" && lifted.Load() && !killed.Load():
			k, err := recordedK(out)
			if err != nil {
				why.Store(fmt.Sprintf("the journal at a recorded part: %v", err))
				killed.Store(true)
				kill()
				return faults.ErrCrash
			}
			n := map[string]int{}
			for _, c := range kept {
				n[c.against(k)]++
			}
			if n["covered"] >= 1 && n["straddled"] >= 1 && n["straddled"]+n["above"] >= 3 {
				killed.Store(true)
				kill()
				return faults.ErrCrash
			}
		}
		return nil
	})
	_, err := Run(context.Background(), durableConfig(url, out, false))
	restore()
	release() // a held request ended with its run's cancellation
	frontierHold.Store(nil)
	if w := why.Load(); w != nil {
		t.Fatalf("%s: %v", w, err)
	}
	if !killed.Load() || !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("not killed (%d seal records at the last ledger sync, frontier released %v): %v", seals.Load(), lifted.Load(), err)
	}
	return out
}

// snapshotCerts counts the live certificates of the damaged snapshot out:
// "missing" (their file gone), else how K stands to them (certSpan.against).
func snapshotCerts(t *testing.T, out string) map[string]int {
	t.Helper()
	b, _, _, _ := ledgerFrames(t, out)
	k, err := recordedK(out)
	if err != nil {
		t.Fatal(err)
	}
	n := map[string]int{}
	for _, c := range ledgerSeals(b) {
		if _, err := os.Stat(filepath.Join(out, order.DurableDir, hex.EncodeToString(c.id)+".spill")); err != nil {
			n["missing"]++
			continue
		}
		n[c.against(k)]++
	}
	return n
}

// TestDurableRepeatedRecovery (Sol 1): a resume is interrupted twice
// inside the durable spill's recovery, each schedule on its own copy of
// the damaged snapshot (damagedSnapshot), and every interruption must
// happen (a point not reached fails the test). Where the state after the
// first interruption still reaches the point, the second is there again:
// before the truncation; after it, before its sync, with the truncation
// lost (simulated power loss: the ledger's earlier bytes restored); before
// the retirements' append; at their sync with the unsynced append lost
// (the ledger cut back to its synced prefix); at a recovery replay's
// start; mid replay (its second captured frame, after earlier rows were
// handed on). Where it cannot (the
// retirements durable), the second is at the first recovery replay. Then
// a resume completes: the part journal is never refused, and the dataset
// is the uninterrupted one.
//
// The snapshots are built by barriers, not sampled (heldSnapshot): one
// whose every certificate lies above K, and one whose K covers a
// certificate (no retirement record: recovery retires it beside the
// missing one, in one append) and lies inside another (its replay skips
// the rows at or below K), checked on the damaged snapshot itself. Every
// schedule runs from the first; the lost retirement append and the
// mid-replay pair run from the second as well. Each snapshot's resume, run
// once uninterrupted on a copy (the dataset must be the uninterrupted
// one), must replay three captured frames or more. That is what the
// mid-replay pair needs: the replay is the drainer's (one encoder: no
// segments), which reaches spill-replay-frame before every captured frame
// whatever K; the first interruption, before the second frame, can retire
// only a certificate whose frames were all handed on (the first frame's: a
// part recorded before the crash can cover it), so two frames remain for
// the second, which the state it left must show (resumed on a copy, to the
// uninterrupted dataset) before it is interrupted again.
func TestDurableRepeatedRecovery(t *testing.T) {
	url, _, _ := slowFrontier(t, "d06/")
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), durableConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	snapshots := map[bool]string{}
	for _, advanced := range []bool{false, true} {
		out := heldSnapshot(t, url, advanced)
		damagedSnapshot(t, out)
		n := snapshotCerts(t, out)
		frames, r := replayedFrames(t, url, ref, out)
		t.Logf("snapshot (advanced K %v): certificates %v; its resume replays %d captured frames: %+v", advanced, n, frames, r)
		if n["missing"] < 1 || (n["covered"] > 0) != advanced || (n["straddled"] > 0) != advanced || frames < 3 {
			t.Fatalf("snapshot (advanced K %v): certificates %v, %d captured frames replayed; want one missing, one covered and one straddled with K advanced, none without, and 3 frames or more", advanced, n, frames)
		}
		snapshots[advanced] = out
	}
	ledger := func(dir string) string { return filepath.Join(dir, order.DurableDir, "ledger") }
	for _, sc := range []struct {
		first, second string
		nth           int64  // the occurrence interrupted (0: the first)
		lose          string // unsynced state lost at the first interruption: "append", "truncation"
		advanced      bool   // from the advanced-K snapshot
	}{
		{"spill-ledger-truncate", "spill-ledger-truncate", 0, "", false},
		{"spill-ledger-truncated", "spill-ledger-truncated", 0, "truncation", false},
		{"spill-ledger-truncated", "spill-replay", 0, "", false},
		{"spill-recovery-retire", "spill-recovery-retire", 0, "", false},
		{"spill-recovery-sync", "spill-recovery-sync", 0, "append", false},
		{"spill-recovery-sync", "spill-replay", 0, "", false},
		{"spill-recovery-unlink", "spill-replay", 0, "", false},
		{"spill-recovery-dirsync", "spill-replay", 0, "", false},
		{"spill-replay", "spill-replay", 0, "", false},
		{"spill-replay-frame", "spill-replay-frame", 2, "", false},
		{"spill-recovery-sync", "spill-recovery-sync", 0, "append", true},
		{"spill-replay-frame", "spill-replay-frame", 2, "", true},
	} {
		out := snapshots[sc.advanced]
		name := sc.first + "+" + sc.second
		if sc.nth > 1 {
			name += fmt.Sprintf("#%d", sc.nth)
		}
		if sc.lose != "" {
			name += "/unsynced-" + sc.lose + "-lost"
		}
		if sc.advanced {
			name = "advanced-K/" + name
		}
		t.Run(name, func(t *testing.T) {
			nth := max(sc.nth, 1)
			interrupt := func(dir string, i int, point string) {
				t.Helper()
				var seen atomic.Int64
				synced := int64(-1)
				var whole []byte
				restore := faults.Set(func(p string) error {
					switch {
					case p == "spill-ledger-truncate" && whole == nil:
						whole, _ = os.ReadFile(ledger(dir)) // durable before the truncation
					case p == "spill-recovery-retire" && synced < 0:
						if fi, err := os.Stat(ledger(dir)); err == nil {
							synced = fi.Size() // the truncated prefix, synced
						}
					}
					if p == point && seen.Add(1) == nth {
						return faults.ErrCrash
					}
					return nil
				})
				sum, err := Run(context.Background(), durableConfig(url, dir, true))
				restore()
				if seen.Load() < nth || !errors.Is(err, faults.ErrCrash) {
					var r any
					if sum.Resources.Spill.Durable != nil {
						r = sum.Resources.Spill.Durable.Resume
					}
					t.Fatalf("interruption %d at %s#%d not reached (%d seen): %v; resume: %+v", i+1, point, nth, seen.Load(), err, r)
				}
				if errors.Is(err, journal.ErrCorrupt) {
					t.Fatalf("%s: the part journal refused: %v", point, err)
				}
				switch {
				case sc.lose == "append":
					if synced < 0 {
						t.Fatal("no synced prefix recorded")
					}
					if err := os.Truncate(ledger(dir), synced); err != nil {
						t.Fatal(err)
					}
				case sc.lose == "truncation":
					// Truncated, never synced: the power loss restores the
					// ledger's durable bytes.
					if whole == nil {
						t.Fatal("no ledger before the truncation")
					}
					if err := os.WriteFile(ledger(dir), whole, 0o644); err != nil {
						t.Fatal(err)
					}
				}
			}
			dir := filepath.Join(t.TempDir(), "ds")
			if err := os.CopyFS(dir, os.DirFS(out)); err != nil {
				t.Fatal(err)
			}
			interrupt(dir, 0, sc.first)
			if sc.second == "spill-replay-frame" {
				// The state the first interruption left resumes to the
				// uninterrupted dataset and still replays nth captured
				// frames (at most the first frame's certificate retired).
				if frames, r := replayedFrames(t, url, ref, dir); frames < nth {
					t.Fatalf("the first interruption left %d captured frames to replay, want %d: %+v", frames, nth, r)
				}
			}
			interrupt(dir, 1, sc.second)
			sum, err := Run(context.Background(), durableConfig(url, dir, true))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("final resume: %+v", sum.Resources.Spill.Durable.Resume)
			sameDataset(t, name, ref, dir)
		})
	}
}

// replayedFrames resumes a copy of the snapshot out, uninterrupted: the
// dataset must be the uninterrupted one (ref); it returns the captured
// frames the resume replayed and its durable account.
func replayedFrames(t *testing.T, url, ref, out string) (int64, any) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "probe")
	if err := os.CopyFS(dir, os.DirFS(out)); err != nil {
		t.Fatal(err)
	}
	var frames atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "spill-replay-frame" {
			frames.Add(1)
		}
		return nil
	})
	sum, err := Run(context.Background(), durableConfig(url, dir, true))
	restore()
	if err != nil {
		t.Fatalf("the damaged snapshot's resume: %v", err)
	}
	sameDataset(t, "damaged snapshot", ref, dir)
	var r any
	if sum.Resources.Spill.Durable != nil {
		r = sum.Resources.Spill.Durable.Resume
	}
	return frames.Load(), r
}

// TestDurableNewAncestorsLost: a power loss before the spill directories'
// entries were synced loses the whole durable spill: the resume relists,
// without an error.
func TestDurableNewAncestorsLost(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	if err := os.RemoveAll(filepath.Join(out, filepath.Dir(order.DurableDir))); err != nil {
		t.Fatal(err)
	}
	sum, err := Run(context.Background(), durableConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "ancestors lost", ref, out)
	if r := sum.Resources.Spill.Durable.Resume; r.Accepted != 0 {
		t.Fatalf("reused %d certificates from nowhere", r.Accepted)
	}
}

// TestDurableCompressionFlip: the crashed run's durable files hold delta
// zstd frames (the default, Y); a resume with another frame format
// (--no-spill-compress: D, --spill-plain: Z, both: R) replays them all
// the same: a durable file's frames are self-describing, and the header's
// row codec version covers all four rows frame types.
func TestDurableCompressionFlip(t *testing.T) {
	for _, flip := range []struct {
		name                string
		plain, uncompressed bool
	}{{"uncompressed", false, true}, {"plain", true, false}, {"plain-uncompressed", true, true}} {
		t.Run(flip.name, func(t *testing.T) {
			url, _, ref, out := durableFixture(t)
			cfg := durableConfig(url, out, true)
			cfg.Limits.Spill.Plain, cfg.Limits.Spill.Uncompressed = flip.plain, flip.uncompressed
			sum, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			sameDataset(t, flip.name, ref, out)
			if sum.Resources.Spill.Durable.Resume.ReplayedRows == 0 {
				t.Fatal("nothing replayed")
			}
		})
	}
}

// TestDurableVersions: a journal of another version is refused before
// recovery changes anything, the durable spill included; the current
// version resumes.
func TestDurableVersions(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	setVersion := func(dir string, v int) {
		path := journal.Path(dir)
		jb, _ := os.ReadFile(path)
		size := binary.LittleEndian.Uint32(jb[:4])
		cur := fmt.Sprintf(`"version":%d`, journal.Version)
		nh := strings.Replace(string(jb[8:8+size]), cur, fmt.Sprintf(`"version":%d`, v), 1)
		frame := make([]byte, 8, 8+len(nh))
		binary.LittleEndian.PutUint32(frame[:4], uint32(len(nh)))
		frame = append(frame, nh...)
		binary.LittleEndian.PutUint32(frame[4:8], crc32.Update(crc32.Checksum(frame[:4], crc32.MakeTable(crc32.Castagnoli)), crc32.MakeTable(crc32.Castagnoli), []byte(nh)))
		os.WriteFile(path, append(frame, jb[8+size:]...), 0o644)
	}
	for _, v := range []int{0, journal.Version + 1} {
		dir := filepath.Join(t.TempDir(), strconv.Itoa(v))
		if err := os.CopyFS(dir, os.DirFS(out)); err != nil {
			t.Fatal(err)
		}
		setVersion(dir, v)
		before := snapshot(t, dir)
		_, err := Run(context.Background(), durableConfig(url, dir, true))
		var ve *journal.VersionError
		if !errors.As(err, &ve) || ve.Found != v {
			t.Fatalf("version %d: %v", v, err)
		}
		if after := snapshot(t, dir); len(after) != len(before) {
			t.Fatalf("version %d: files changed", v)
		}
		if got := durableFiles(t, dir); len(got) != len(durableFiles(t, out)) {
			t.Fatalf("version %d: durable spill changed", v)
		}
	}
	if _, err := Run(context.Background(), durableConfig(url, out, true)); err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "current version", ref, out)
}

// TestDurableChangedSource: retained observation. Between the crash and the
// resume a key inside a reused certificate is deleted and one added there,
// and the same inside a gap: the certificate's rows are replayed as
// observed (the deleted key stays, the added one is missed), the gap is
// listed as it is now.
func TestDurableChangedSource(t *testing.T) {
	url, b, _, out := durableFixture(t)
	// The certificates the resume will accept: recover a copy.
	cp := filepath.Join(t.TempDir(), "copy")
	if err := os.CopyFS(cp, os.DirFS(out)); err != nil {
		t.Fatal(err)
	}
	st, err := journal.Read(cp)
	if err != nil {
		t.Fatal(err)
	}
	var k *string
	if n := len(st.Records); n > 0 {
		kk := string(st.Records[n-1].Part.MaxKey)
		k = &kk
	}
	root, _ := os.OpenRoot(cp)
	defer root.Close()
	c, err := order.RecoverDurable(root, nil, st.Header.RunUUID, st.Header.Identity.Hash(), st.Header.Identity.SchemaID, k)
	if err != nil {
		t.Fatal(err)
	}
	c.Start(order.Limits(1000<<20, 0), 0, nil)
	spans := c.Spans()
	c.Close()
	if len(spans) == 0 {
		t.Fatal("no certificate to reuse")
	}
	all := gcstest.Keys(30000)
	slices.Sort(all)
	in := func(x string) bool {
		for _, s := range spans {
			if x > max(s.Lo, *k) && (s.HiOpen || x <= s.Hi) {
				return true
			}
		}
		return false
	}
	var inCert, inGap string
	for _, x := range all {
		switch {
		case k != nil && x <= *k:
		case in(x) && inCert == "":
			inCert = x
		case !in(x) && inGap == "":
			inGap = x
		}
	}
	if inCert == "" || inGap == "" {
		t.Fatalf("no key in a certificate (%q) or a gap (%q)", inCert, inGap)
	}
	for _, x := range []string{inCert, inGap} {
		if _, err := b.DeleteObject("bkt", x); err != nil {
			t.Fatal(err)
		}
	}
	added := []string{inCert + "~added", inGap + "~added"}
	for _, x := range added {
		if _, err := b.PutObject("bkt", x, map[string]string{}, strings.NewReader("x"), 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	sum, err := Run(context.Background(), durableConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("K %q, spans %+v, in cert %q, in gap %q, resume %+v", *k, spans, inCert, inGap, sum.Resources.Spill.Durable.Resume)
	got := checkContract(t, out)
	has := func(x string) bool { _, ok := slices.BinarySearch(got, x); return ok }
	switch {
	case !has(inCert):
		t.Fatal("a key deleted inside a reused certificate is gone: not retained observation")
	case has(added[0]):
		t.Fatal("a key added inside a reused certificate was listed")
	case has(inGap):
		t.Fatal("a key deleted inside a gap is still there")
	case !has(added[1]):
		t.Fatal("a key added inside a gap is missing")
	}
}

// TestDurableShutdownOrder (Sol 2): the final part's publication held after
// the order stage has shut down, with a cancellation meanwhile: every K
// callback the cache applies precedes its detachment, the flusher runs no
// cycle after it is joined, the ledger closes after the join, and the
// waker stopped before the held publication; the run, failed or not,
// resumes to the uninterrupted dataset.
func TestDurableShutdownOrder(t *testing.T) {
	url, _, ref, _ := durableFixture(t)
	out := filepath.Join(t.TempDir(), "held")
	var mu sync.Mutex
	var events []string
	var stopped atomic.Bool
	defer order.SetDurableEvents(func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
		if e == "waker-stopped" {
			stopped.Store(true)
		}
	})()
	barrier, held := make(chan struct{}), make(chan struct{}, 1)
	defer faults.Set(func(p string) error {
		if p == "part-closed" && stopped.Load() {
			mu.Lock()
			events = append(events, "publication-held")
			mu.Unlock()
			select {
			case held <- struct{}{}:
			default:
			}
			<-barrier
		}
		return nil
	})()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-held
		cancel()
		time.Sleep(20 * time.Millisecond)
		close(barrier)
	}()
	_, err := Run(ctx, durableConfig(url, out, false))
	cancel()
	mu.Lock()
	ev := slices.Clone(events)
	mu.Unlock()
	at := func(e string) int { return slices.Index(ev, e) }
	last := func(e string) int {
		for i := len(ev) - 1; i >= 0; i-- {
			if ev[i] == e {
				return i
			}
		}
		return -1
	}
	if at("publication-held") < 0 || at("waker-stopped") > at("publication-held") {
		t.Fatalf("the publication was not held after the order stage's shutdown: %v", ev)
	}
	if last("setk") > at("detach") || at("detach") > at("close") || at("close") > at("flusher-joined") ||
		last("cycle") > at("flusher-joined") || at("flusher-joined") > at("ledger-closed") {
		t.Fatalf("shutdown order: %v", ev)
	}
	if err != nil {
		if _, err := Run(context.Background(), durableConfig(url, out, true)); err != nil {
			t.Fatal(err)
		}
	}
	sameDataset(t, "held publication", ref, out)
}

// TestDurableSealENOSPC: no space for a seal's sync stops the run as a
// full disk (resumable), and the resume completes the dataset.
func TestDurableSealENOSPC(t *testing.T) {
	url, _, log := slowFrontier(t, "d06/")
	_ = log
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), durableConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	var n atomic.Int64
	restore := faults.SetIO(func(op, name string, b []byte) (int, error) {
		if op == "sync" && strings.HasSuffix(name, ".spill") && n.Add(1) == 1 {
			return 0, syscall.ENOSPC
		}
		return 0, nil
	})
	_, err := Run(context.Background(), durableConfig(url, out, false))
	restore()
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("run error %v", err)
	}
	if _, err := Run(context.Background(), durableConfig(url, out, true)); err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "seal ENOSPC", ref, out)
}

// TestDurableOverwrite: --overwrite starts over on an output holding a
// durable spill, which goes with the rest of the workspace.
func TestDurableOverwrite(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	cfg := durableConfig(url, out, false)
	cfg.Output.Replace = true
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "overwrite", ref, out)
	if _, err := os.Stat(filepath.Join(out, order.DurableDir)); !os.IsNotExist(err) {
		t.Fatalf("the durable spill survived: %v", err)
	}
}

// TestDurableUnknownCodec: durable files whose header names a row codec
// this run does not replay, every checksum valid (the header frame's, the
// trailer's prior-bytes CRC, the whole file's CRC in its ledger record), so
// the codec check itself decides: recovery rejects them (rejected.codec)
// and the resume lists their intervals as gaps, instead of refusing at
// replay; the dataset is the uninterrupted one. Files no record names (a
// range still spilling at the kill has no trailer yet, and a seal not yet
// appended has no record) are left as they are: the resume unlinks them.
// Whether the kill left one is timing; a partial file (a header, no
// trailer) under a name no record holds is planted so that one is
// always there, and its unlink is checked by name.
func TestDurableUnknownCodec(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	files := durableFiles(t, out)
	if len(files) == 0 {
		t.Fatal("no durable file after the crash")
	}
	recorded, unrecorded := 0, 0
	var sealed string
	for _, name := range files {
		if recodec(t, out, name) {
			recorded++
			sealed = name
		} else {
			unrecorded++
		}
	}
	if recorded == 0 {
		t.Fatal("no durable file has a ledger record")
	}
	partial := plantPartial(t, out, sealed)
	unrecorded++
	sum, err := Run(context.Background(), durableConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "unknown codec", ref, out)
	r := sum.Resources.Spill.Durable.Resume
	if r.Rejected["codec"] == 0 || r.Accepted != 0 || r.Rejected["corrupt"] != 0 || r.Orphans < int64(unrecorded) {
		t.Fatalf("resume %+v (%d files unrecorded)", r, unrecorded)
	}
	if _, err := os.Stat(filepath.Join(out, order.DurableDir, partial)); !os.IsNotExist(err) {
		t.Fatalf("the unrecorded partial file survived the resume: %v", err)
	}
}

// plantPartial writes a durable file that a kill mid-range would leave,
// the header frame of sealed and nothing after it, under a fresh name
// no ledger record holds, and returns the name.
func plantPartial(t *testing.T, out, sealed string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(out, order.DurableDir, sealed))
	if err != nil {
		t.Fatal(err)
	}
	n0 := int(binary.LittleEndian.Uint32(b))
	name := hex.EncodeToString([]byte("unrecorded-file!")) + ".spill" // 16 bytes, no run's random ID
	if err := os.WriteFile(filepath.Join(out, order.DurableDir, name), b[:8+n0], 0o644); err != nil {
		t.Fatal(err)
	}
	return name
}

// recodec rewrites durable file name in out to name another row codec (the
// codec ID's last byte changed, its length kept), with every checksum made
// valid again: the header frame's CRC, the trailer's CRC of the bytes
// before it, and the whole file's CRC in its ledger record. False: no
// record names the file (an orphan the resume unlinks), which is left as
// it is; it may have no trailer (its range was still spilling at the
// kill). A recorded file was sealed, so it ends in its tail.
func recodec(t *testing.T, out, name string) bool {
	t.Helper()
	tab := crc32.MakeTable(crc32.Castagnoli)
	reseal := func(fr []byte) {
		binary.LittleEndian.PutUint32(fr[4:8], crc32.Update(crc32.Checksum(fr[:4], tab), tab, fr[8:]))
	}
	id, _ := hex.DecodeString(strings.TrimSuffix(name, ".spill"))
	lp := filepath.Join(out, order.DurableDir, "ledger")
	lb, _ := os.ReadFile(lp)
	var recs [][]byte // the frames of the records naming the file
	for at := 0; at+8 <= len(lb); {
		n := int(binary.LittleEndian.Uint32(lb[at:]))
		if at+8+n > len(lb) {
			break
		}
		fr := lb[at : at+8+n]
		if fr[8] == 'S' && bytes.Equal(fr[9:25], id) {
			recs = append(recs, fr)
		}
		at += 8 + n
	}
	if len(recs) == 0 {
		return false
	}
	path := filepath.Join(out, order.DurableDir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 16 || string(b[len(b)-8:]) != "SWATHDS1" {
		t.Fatalf("%s has a ledger record and no tail (%d bytes)", name, len(b))
	}
	n0 := int(binary.LittleEndian.Uint32(b))
	body := b[9 : 8+n0]    // the H frame's body
	pos := 8 + 1 + 16 + 16 // magic, version, run UUID, file ID
	for range 2 {          // the identity digest, then the codec ID
		l, k := binary.Uvarint(body[pos:])
		pos += k
		if l == 0 {
			t.Fatal("empty header string")
		}
		pos += int(l)
	}
	body[pos-1] ^= 0x20 // the codec ID's last byte
	reseal(b[:8+n0])
	off := int(binary.LittleEndian.Uint64(b[len(b)-16:]))
	nt := int(binary.LittleEndian.Uint32(b[off:]))
	tr := b[off : off+8+nt]
	binary.LittleEndian.PutUint32(tr[len(tr)-4:], crc32.Checksum(b[:off], tab))
	reseal(tr)
	sum := crc32.Checksum(b, tab)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, fr := range recs {
		binary.LittleEndian.PutUint32(fr[len(fr)-4:], sum)
		reseal(fr)
	}
	os.WriteFile(lp, lb, 0o644)
	return true
}

// TestDurableKillMatrix kills a durable-spill run (an injected crash that
// skips all cleanup) at each of its steps (faults.SpillPoints: a durable
// file's first write (its header, with the capture start), later frames and
// the trailer, its seal's fsync,
// the flusher's directory sync, ledger append, ledger sync and unlink), at
// the first and the fourth occurrence, on the slow-frontier fixture built
// to reach them: a point not reached fails the test. Then torn writes:
// each kind of durable file write and a ledger append cut part way through
// (a kill mid-write), and the unsynced state lost after a crash at the
// ledger sync and at a seal (power loss: the ledger cut back to its last
// sync, the unsealed file cut short). Every resume completes to the
// uninterrupted dataset, and reuses only certificates whose record
// survived in the prefix (the resume never refuses).
func TestDurableKillMatrix(t *testing.T) {
	url, _, _ := slowFrontier(t, "d06/")
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), durableConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	resume := func(t *testing.T, out string) {
		t.Helper()
		sum, err := Run(context.Background(), durableConfig(url, out, true))
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		sameDataset(t, out, ref, out)
		if d := sum.Resources.Spill.Durable; d != nil && d.Resume != nil {
			t.Logf("resume: %+v", *d.Resume)
		}
	}
	for _, point := range faults.SpillPoints {
		nths := []int64{1, 4}
		if point == "spill-dir-sync" {
			nths = []int64{1, 2} // spill/ and spill/durable/: two per run
		}
		for _, nth := range nths {
			t.Run(fmt.Sprintf("%s#%d", point, nth), func(t *testing.T) {
				done := gateSeals(t)
				out := filepath.Join(t.TempDir(), "ds")
				crashDurable(t, durableConfig(url, out, false), point, nth)
				done()
				resume(t, out)
			})
		}
	}
	// Torn writes: the nth write carrying a frame of a kind (or a tail) is
	// cut at an explicit offset inside that frame (or the tail), then the
	// process dies; the cut is checked to lie inside it.
	for _, tc := range []struct {
		kind string // H, C, rows, T, tail, ledger
		nth  int64
	}{{"H", 2}, {"C", 2}, {"rows", 4}, {"T", 2}, {"tail", 2}, {"ledger", 2}} {
		t.Run("torn-"+tc.kind, func(t *testing.T) {
			done := gateSeals(t)
			out := filepath.Join(t.TempDir(), "ds")
			var seen atomic.Int64
			var cut, lo, hi int
			restore := faults.SetIO(func(op, name string, b []byte) (int, error) {
				if op != "write" || !strings.HasSuffix(name, map[bool]string{true: "ledger", false: ".spill"}[tc.kind == "ledger"]) {
					return 0, nil
				}
				k := tc.kind
				if k == "ledger" {
					k = "any"
				}
				c, l, h := tearAt(b, k)
				if c < 0 || seen.Add(1) != tc.nth {
					return 0, nil
				}
				cut, lo, hi = c, l, h
				if g := runGate.Load(); g != nil {
					g.stop.Store(true)
				}
				return c, faults.ErrCrash
			})
			_, err := Run(context.Background(), durableConfig(url, out, false))
			restore()
			done()
			if seen.Load() < tc.nth || !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("torn %s write not injected (%d seen): %v", tc.kind, seen.Load(), err)
			}
			if cut <= lo || cut >= hi {
				t.Fatalf("cut at %d, not inside the %s at [%d, %d)", cut, tc.kind, lo, hi)
			}
			resume(t, out)
		})
	}
	// Unsynced state lost: crashed at the ledger's sync, its append (not yet
	// synced) is lost; crashed at a seal (the file not yet synced), the
	// file's last bytes are lost.
	t.Run("lost-ledger-append", func(t *testing.T) {
		done := gateSeals(t)
		out := filepath.Join(t.TempDir(), "ds")
		var before int64 = -1
		var seen atomic.Int64
		restore := faults.Set(gated(func(p string) error {
			switch {
			case p == "spill-ledger-append" && seen.Load() == 1:
				if fi, err := os.Stat(filepath.Join(out, order.DurableDir, "ledger")); err == nil {
					before = fi.Size()
				}
			case p == "spill-ledger-sync" && seen.Add(1) == 2:
				return faults.ErrCrash
			}
			return nil
		}))
		_, err := Run(context.Background(), durableConfig(url, out, false))
		restore()
		done()
		if !errors.Is(err, faults.ErrCrash) || before < 0 {
			t.Fatalf("crash at the second ledger sync not injected (prefix %d): %v", before, err)
		}
		if err := os.Truncate(filepath.Join(out, order.DurableDir, "ledger"), before); err != nil {
			t.Fatal(err)
		}
		resume(t, out)
	})
	t.Run("lost-unsealed-file", func(t *testing.T) {
		done := gateSeals(t)
		out := filepath.Join(t.TempDir(), "ds")
		crashDurable(t, durableConfig(url, out, false), "spill-seal", 2)
		done()
		for _, name := range durableFiles(t, out) {
			p := filepath.Join(out, order.DurableDir, name)
			if fi, err := os.Stat(p); err == nil && fi.Size() > 64 {
				os.Truncate(p, fi.Size()-32)
			}
		}
		resume(t, out)
	})
}

// TestDurableLedgerTornWrite (Sol 3): a ledger append torn part way (a
// partial write that fails) while parts are being published: the ledger
// latches failed (no new certificate) and the run goes on publishing
// (order's TestDurableLedgerFailureEvicts and TestDurableEviction hold the
// unlinks and the credit after the failure); killed
// after its next part record, the resume reads the ledger's prefix up to
// the torn frame (dropping it), relists what lost its record, and the
// dataset is the uninterrupted one.
func TestDurableLedgerTornWrite(t *testing.T) {
	url, _, _ := slowFrontier(t, "d06/")
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), durableConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	// The frontier is held until the tear, so all but the seed page's part
	// records follow it (the premise is a barrier, not a race: a late
	// flusher used to leave too few part records after it), and the tear
	// lands inside the append's first frame, never on a frame boundary.
	h := &hold{release: make(chan struct{})}
	runHold := func() func() { frontierHold.Store(h); return func() { frontierHold.Store(nil) } }
	var writes, partsBefore atomic.Int64
	var tornAt atomic.Bool
	var cut, hi int
	restoreIO := faults.SetIO(func(op, name string, b []byte) (int, error) {
		if op == "write" && strings.HasSuffix(name, "ledger") && writes.Add(1) == 2 {
			cut, _, hi = tearAt(b, "any")
			tornAt.Store(true)
			close(h.release)
			return cut, errors.New("injected EIO")
		}
		return 0, nil
	})
	var after atomic.Int64
	restore := faults.Set(func(p string) error {
		// Publication goes on after the failure: two more part records,
		// then the kill.
		if p != "journal-append" {
			return nil
		}
		if !tornAt.Load() {
			partsBefore.Add(1)
		} else if after.Add(1) == 2 {
			return faults.ErrCrash
		}
		return nil
	})
	unhold := runHold()
	_, err := Run(context.Background(), durableConfig(url, out, false))
	unhold()
	restore()
	restoreIO()
	if h.late.Load() || !tornAt.Load() || !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("torn ledger write %v (frontier held in vain %v), then the kill: %v", tornAt.Load(), h.late.Load(), err)
	}
	if cut <= 8 || cut >= hi {
		t.Fatalf("cut at %d of a frame ending at %d", cut, hi)
	}
	t.Logf("%d part records before the tear (the seed page's)", partsBefore.Load())
	sum, err := Run(context.Background(), durableConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "torn ledger", ref, out)
	r := sum.Resources.Spill.Durable.Resume
	if r.LedgerDroppedBytes == 0 {
		t.Fatalf("the torn frame was not dropped: %+v", r)
	}
	t.Logf("resume after the torn ledger: %+v", r)
}

// ledgerFDs counts this process's open descriptors on out's ledger.
func ledgerFDs(t *testing.T, out string) int {
	t.Helper()
	want, _ := filepath.EvalSymlinks(filepath.Join(out, order.DurableDir))
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc/self/fd")
	}
	n := 0
	for _, e := range ents {
		if l, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && l == filepath.Join(want, "ledger") {
			n++
		}
	}
	return n
}

// TestDurableFailedRecoveryCloses: a resume whose durable spill fails in
// recovery (interrupted at the retirements' sync) or in Start (its cap 0
// retires every accepted certificate, interrupted there) leaves no
// descriptor on the ledger after Run returns, and writes nothing more.
func TestDurableFailedRecoveryCloses(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	// A live certificate's file removed: recovery always retires it, so
	// its sync is reached (without the damage it was reached only when K
	// happened to cover a certificate).
	damagedSnapshot(t, out)
	occurrences := func(dir string, cfg func(string) Config, point string) int64 {
		cp := filepath.Join(t.TempDir(), "probe")
		if err := os.CopyFS(cp, os.DirFS(dir)); err != nil {
			t.Fatal(err)
		}
		var n atomic.Int64
		defer faults.Set(func(p string) error {
			if p == point {
				n.Add(1)
			}
			return nil
		})()
		if _, err := Run(context.Background(), cfg(cp)); err != nil {
			t.Fatal(err)
		}
		return n.Load()
	}
	for _, tc := range []struct {
		name, point string
		cfg         func(string) Config
		last        bool // the point's last occurrence (Start's, after recovery's)
	}{
		{"recovery", "spill-recovery-sync", func(d string) Config { return durableConfig(url, d, true) }, false},
		{"start", "spill-recovery-retire", func(d string) Config {
			cfg := durableConfig(url, d, true)
			cfg.Limits.Spill.Max = 8 << 20 // below 16 MiB: cap 0, Start retires every accepted certificate
			return cfg
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ds")
			if err := os.CopyFS(dir, os.DirFS(out)); err != nil {
				t.Fatal(err)
			}
			nth := int64(1)
			if tc.last {
				nth = occurrences(dir, tc.cfg, tc.point)
				if nth == 0 {
					t.Fatalf("%s not reached", tc.point)
				}
			}
			crashDurable(t, tc.cfg(dir), tc.point, nth)
			if n := ledgerFDs(t, dir); n != 0 {
				t.Fatalf("%d descriptors left on the ledger after Run returned", n)
			}
			if _, err := Run(context.Background(), durableConfig(url, dir, true)); err != nil {
				t.Fatal(err)
			}
			sameDataset(t, tc.name, ref, dir)
		})
	}
}

// tearAt is where to cut write b inside its first frame of kind k (a frame
// type "H", "C", "T"; "rows": any other spill frame; "any": the first
// frame; "tail": the 16-byte tail after a trailer), with that frame's (or
// tail's) bounds; -1 if b holds none. The cut keeps the frame's header and
// half its payload: a torn frame, never a frame boundary.
func tearAt(b []byte, k string) (cut, lo, hi int) {
	off, last := 0, ""
	for off+9 <= len(b) {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		if n < 2 || off+8+n > len(b) {
			break
		}
		typ := string(b[off+8])
		last = typ
		hit := k == "any" || k == typ || (k == "rows" && typ != "H" && typ != "C" && typ != "T")
		if hit {
			return off + 8 + n/2, off, off + 8 + n
		}
		off += 8 + n
	}
	if k == "tail" && last == "T" && len(b)-off == 16 {
		return off + 8, off, len(b)
	}
	return -1, 0, 0
}

// TestDurableResumeSmallerBudget: killed at a ledger sync once four
// certificates are sealed, its frontier held, a run with a 1 MiB sort
// buffer and 32 workers resumes with the 128 KiB minimum, 4 workers and a
// 16 MiB spill cap (the durable spill's least), replaying its
// certificates through the spill read handles; the dataset is the
// uninterrupted run's, byte for byte. The same with --all-versions hot
// groups (a group's versions consecutive, never split at a certificate),
// killed once eight are sealed. Neither is killed at its first unlink: it
// can come after the drain has published every certificate (the flusher's
// cycle lagging a fast drain), leaving nothing to replay.
func TestDurableResumeSmallerBudget(t *testing.T) {
	smaller := func(cfg Config) Config {
		cfg.Limits.RowBudget, cfg.Limits.Workers, cfg.Limits.Spill.Max = MinRowBudget, 4, 16<<20
		return cfg
	}
	handles := func(t *testing.T, sum Report) {
		t.Helper()
		sp := sum.Resources.Spill
		if sp == nil || sp.Durable == nil || sp.Durable.Resume == nil || sp.Durable.Resume.ReplayedRows == 0 {
			t.Fatalf("nothing replayed: %+v", sp)
		}
		if h := sp.ReadHandles; h.PeakOpen > order.MaxSpillReaders {
			t.Fatalf("read handles: %+v", h)
		}
		t.Logf("resume: %d rows replayed, read handles %+v, frames %+v", sp.Durable.Resume.ReplayedRows, sp.ReadHandles, sp.FrameRows)
	}
	t.Run("objects", func(t *testing.T) {
		url, _, _ := slowFrontier(t, "d06/")
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), durableConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(t.TempDir(), "ds")
		cfg := durableConfig(url, out, false)
		cfg.Limits.RowBudget = 1 << 20
		// The frontier is held until the kill, so K stays below the
		// certificates the ledger holds then, four or more: killed at the
		// first unlink instead, a fast drain could publish every
		// certificate before the flusher's first retirement and leave
		// nothing to replay (ubuntu-24.04-arm CI).
		h := &hold{release: make(chan struct{})}
		frontierHold.Store(h)
		var sealed atomic.Int64
		var killed atomic.Bool
		restore := faults.Set(func(p string) error {
			if p == "spill-seal" {
				sealed.Add(1)
			}
			if p == "spill-ledger-sync" && sealed.Load() >= 4 && killed.CompareAndSwap(false, true) {
				close(h.release)
				return faults.ErrCrash
			}
			return nil
		})
		_, err := Run(context.Background(), cfg)
		restore()
		frontierHold.Store(nil)
		if h.late.Load() || !errors.Is(err, faults.ErrCrash) {
			t.Fatalf("not killed with four certificates sealed (%d sealed, frontier held in vain %v): %v", sealed.Load(), h.late.Load(), err)
		}
		sum, err := Run(context.Background(), smaller(durableConfig(url, out, true)))
		if err != nil {
			t.Fatal(err)
		}
		sameDataset(t, "resume", ref, out)
		handles(t, sum)
	})
	t.Run("versions", func(t *testing.T) {
		api := newHotVersions(t)
		defer order.SetDurableEvents(api.event)()
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), hotVersionsConfig(api, ref, false)); err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(t.TempDir(), "ds")
		cfg := hotVersionsConfig(api, out, false)
		cfg.Limits.RowBudget = 1 << 20
		// K stays below every certificate while the frontier is held, so
		// the ones the ledger holds at the kill, eight or more, are all
		// replayed.
		g := &versionsGate{release: make(chan struct{}), hold: true}
		api.gate.Store(g)
		var killed atomic.Bool
		restore := faults.Set(gated(func(p string) error {
			if p == "spill-ledger-sync" && g.sealed.Load() >= 8 && killed.CompareAndSwap(false, true) {
				return faults.ErrCrash
			}
			return nil
		}))
		_, err := Run(context.Background(), cfg)
		restore()
		if !errors.Is(err, faults.ErrCrash) {
			t.Fatalf("not killed with eight certificates sealed (%d sealed): %v", g.sealed.Load(), err)
		}
		sum, err := Run(context.Background(), smaller(hotVersionsConfig(api, out, true)))
		if err != nil {
			t.Fatal(err)
		}
		sameDataset(t, "resume", ref, out)
		handles(t, sum)
	})
}

// TestDurableSameDiskDefaultCap: with the default spill cap (half the free
// space after the reserve) a same-disk resume finds the free space short
// by the certified files it kept. Shaped as 100 GiB free when the killed
// run started and 40 GiB retained: without counting them the resumed cap
// is 30 GiB and Start rejects a quarter of the retained bytes'
// certificates (rejected.budget); orderOptions counts them as free, so the
// cap is the killed run's, 50 GiB, and every certificate that fit it is
// accepted. The retained bytes are the crashed fixture's (40 units of
// them) and the free space the resume finds is set (freeBytes); the slots
// are the fixture's 64 MiB cap's, so only the byte budget is under test.
// Recovery itself is bounded by the format, never by the free space it
// finds (order.TestDurableRecoveryBounds).
func TestDurableSameDiskDefaultCap(t *testing.T) {
	_, _, _, out := durableFixture(t)
	st, err := journal.Read(out)
	if err != nil {
		t.Fatal(err)
	}
	var k *string
	if n := len(st.Records); n > 0 {
		kk := string(st.Records[n-1].Part.MaxKey)
		k = &kk
	}
	lock, err := journal.TakeLock(out)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	root, _ := os.OpenRoot(out)
	defer root.Close()
	c, err := order.RecoverDurable(root, nil, st.Header.RunUUID, st.Header.Identity.Hash(), st.Header.Identity.SchemaID, k)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	retained, accepted := c.AcceptedBytes(), c.Reused()
	if accepted == 0 {
		t.Fatal("no certificate to keep")
	}
	unit := (retained + 39) / 40
	free := 100*unit - retained // the free space the resume finds
	old := freeBytes
	t.Cleanup(func() { freeBytes = old })
	freeBytes = func(string) int64 { return free }
	if uncounted := order.DiskCap(free, 0, 2); uncounted >= retained {
		t.Fatalf("shape: the uncounted cap %d holds the %d retained bytes", uncounted, retained)
	}
	cfg := durableConfig("s3://unused", out, true)
	cfg.Limits.Spill = SpillLimits{}
	oopt, _, err := orderOptions(cfg, &runJournal{lock: lock, cache: c}, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := order.DiskCap(free+retained, 0, 2); oopt.SpillMax != want || oopt.SpillShare != 2 {
		t.Fatalf("resumed cap %d, want %d (free %d, retained %d)", oopt.SpillMax, want, free, retained)
	}
	if err := c.Start(order.Limits(64<<20, 0), oopt.SpillMax, nil); err != nil {
		t.Fatal(err)
	}
	if d := c.Report(); d.Resume.Rejected["budget"] != 0 || c.Reused() != accepted {
		t.Fatalf("cap %d for %d retained bytes: kept %d of %d, rejected %v", oopt.SpillMax, retained, c.Reused(), accepted, d.Resume.Rejected)
	}
}
