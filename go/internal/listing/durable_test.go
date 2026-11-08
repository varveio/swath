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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// reqLog records the listing requests a fake S3 endpoint served: their
// start-after (empty for a continuation) and count.
type reqLog struct {
	mu     sync.Mutex
	starts []string
	n      atomic.Int64
	on     atomic.Bool
}

// slowFrontier serves the durable-spill fixture: 8,000 keys under nine
// folders, every request 1 ms, and a request whose start lies below slow
// 25 ms more, so the first tiles are a slow frontier and the rest race
// ahead of it and spill under a small budget.
func slowFrontier(t *testing.T, slow string) (string, *s3mem.Backend, *reqLog) {
	t.Helper()
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	b := s3test.Backend(t, gcstest.Keys(30000), s3mem.WithTimeSource(&step{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}))
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
			if h := frontierHold.Load(); h != nil && start == "" && q.Get("continuation-token") == "" && h.seen.Add(1) == 2 {
				// The frontier's first request (the second listing from
				// the start: the seed's is the first) waits for the
				// hold's release.
				select {
				case <-h.release:
				case <-r.Context().Done():
				case <-time.After(20 * time.Second):
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
	seen    atomic.Int64
	late    atomic.Bool
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
// after its second part, the resume replays the certificates that K does
// not cover instead of listing them, lists only the gaps, and the dataset
// is byte-identical to an uninterrupted run. No request of the resume
// starts inside a replayed certificate's [cs, ce).
func TestDurableSpillResume(t *testing.T) {
	datasettest.RequireJournal(t)
	url, _, log := slowFrontier(t, "d06/")
	ref := filepath.Join(t.TempDir(), "ref")
	cold, err := Run(context.Background(), durableConfig(url, ref, false))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cold durable: %+v", cold.Resources.Spill.Durable)
	if d := cold.Resources.Spill.Durable; d == nil || d.SealedFiles == 0 {
		t.Fatalf("no certificate sealed: %+v", cold.Resources.Spill)
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
	t.Logf("cold: %d sealed; resume: accepted %d, replayed %d rows, skipped %d, gaps %d, %d list requests (rows listed %d, retained %d)",
		cold.Resources.Spill.Durable.SealedFiles, d.Resume.Accepted, d.Resume.ReplayedRows, d.Resume.ReplaySkippedRows, d.Resume.Gaps, log.n.Load(), sum.Output.RowsEmitted, sum.Output.RowsRetained)
	if want := sum.Output.RowsEmitted; want <= d.Resume.ReplayedRows {
		t.Fatalf("listed %d rows, replayed %d", want, d.Resume.ReplayedRows)
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
	return b, offs, types, ids
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
// The replay schedules need a snapshot whose resume replays two captured
// frames or more. How many certificates the fixture's crash leaves above
// K, and how many survive the damage, depends on how far the ranges ahead
// of the frontier got before the first retirement: on a loaded runner
// none, or one single-page certificate, so the replay points did not
// exist and the run completed without them (#292). The snapshot is
// therefore resumed once, uninterrupted, on a copy first (the product
// contract: the dataset is the uninterrupted one, whatever was replayed),
// counting the captured frames replayed; a snapshot with fewer than two
// is crashed again, a bounded number of times, and never skipped.
func TestDurableRepeatedRecovery(t *testing.T) {
	datasettest.RequireJournal(t)
	url, _, ref, out := durableFixture(t)
	const tries = 6
	for try := 1; ; try++ {
		damagedSnapshot(t, out)
		frames, r := replayedFrames(t, url, ref, out)
		t.Logf("snapshot %d: its resume replays %d captured frames: %+v", try, frames, r)
		if frames >= 2 {
			break
		}
		if try == tries {
			t.Fatalf("%d snapshots, none replaying two captured frames: the replay schedules cannot be reached", tries)
		}
		out = filepath.Join(t.TempDir(), "ds")
		crashDurable(t, durableConfig(url, out, false), "spill-unlink", 1)
	}
	ledger := func(dir string) string { return filepath.Join(dir, order.DurableDir, "ledger") }
	for _, sc := range []struct {
		first, second string
		nth           int64  // the occurrence interrupted (0: the first)
		lose          string // unsynced state lost at the first interruption: "append", "truncation"
	}{
		{"spill-ledger-truncate", "spill-ledger-truncate", 0, ""},
		{"spill-ledger-truncated", "spill-ledger-truncated", 0, "truncation"},
		{"spill-ledger-truncated", "spill-replay", 0, ""},
		{"spill-recovery-retire", "spill-recovery-retire", 0, ""},
		{"spill-recovery-sync", "spill-recovery-sync", 0, "append"},
		{"spill-recovery-sync", "spill-replay", 0, ""},
		{"spill-recovery-unlink", "spill-replay", 0, ""},
		{"spill-recovery-dirsync", "spill-replay", 0, ""},
		{"spill-replay", "spill-replay", 0, ""},
		{"spill-replay-frame", "spill-replay-frame", 2, ""},
	} {
		name := sc.first + "+" + sc.second
		if sc.nth > 1 {
			name += fmt.Sprintf("#%d", sc.nth)
		}
		if sc.lose != "" {
			name += "/unsynced-" + sc.lose + "-lost"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "ds")
			if err := os.CopyFS(dir, os.DirFS(out)); err != nil {
				t.Fatal(err)
			}
			nth := max(sc.nth, 1)
			for i, point := range []string{sc.first, sc.second} {
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
	datasettest.RequireJournal(t)
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
// (--spill-compress=false: D, --spill-plain: Z, both: R) replays them all
// the same: a durable file's frames are self-describing, and the header's
// row codec version covers all four rows frame types.
func TestDurableCompressionFlip(t *testing.T) {
	datasettest.RequireJournal(t)
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

// TestDurableVersions: a version 7 journal resumes with durable spill off,
// writing nothing of version 8; a version 9 journal is refused before
// recovery changes anything.
func TestDurableVersions(t *testing.T) {
	datasettest.RequireJournal(t)
	url, _, ref, out := durableFixture(t)
	setVersion := func(dir string, v int) {
		path := journal.Path(dir)
		jb, _ := os.ReadFile(path)
		size := binary.LittleEndian.Uint32(jb[:4])
		hdr := string(jb[8 : 8+size])
		i := strings.Index(hdr, `,"run_uuid"`)
		cur := fmt.Sprintf(`"version":%d`, journal.Version)
		nh := strings.Replace(hdr, cur, fmt.Sprintf(`"version":%d`, v), 1)
		if v == 7 && i > 0 {
			j := strings.Index(nh, `,"run_uuid"`)
			k := strings.Index(nh[j+1:], `"`) // the key's closing quote
			_ = k
			end := strings.Index(nh[j:], `}`)
			nh = nh[:j] + nh[j+end:]
		}
		frame := make([]byte, 8, 8+len(nh))
		binary.LittleEndian.PutUint32(frame[:4], uint32(len(nh)))
		frame = append(frame, nh...)
		binary.LittleEndian.PutUint32(frame[4:8], crc32.Update(crc32.Checksum(frame[:4], crc32.MakeTable(crc32.Castagnoli)), crc32.MakeTable(crc32.Castagnoli), []byte(nh)))
		os.WriteFile(path, append(frame, jb[8+size:]...), 0o644)
	}
	nine := filepath.Join(t.TempDir(), "nine")
	if err := os.CopyFS(nine, os.DirFS(out)); err != nil {
		t.Fatal(err)
	}
	setVersion(nine, 9)
	before := snapshot(t, nine)
	_, err := Run(context.Background(), durableConfig(url, nine, true))
	var ve *journal.VersionError
	if !errors.As(err, &ve) || ve.Found != 9 {
		t.Fatalf("version 9: %v", err)
	}
	if after := snapshot(t, nine); len(after) != len(before) {
		t.Fatalf("version 9: files changed")
	}
	if got := durableFiles(t, nine); len(got) != len(durableFiles(t, out)) {
		t.Fatalf("version 9: durable spill changed")
	}
	setVersion(out, 7)
	sum, err := Run(context.Background(), durableConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "version 7", ref, out)
	if sp := sum.Resources.Spill; sp.Durable != nil || sp.DurableOff != "journal version 7" {
		t.Fatalf("version 7 resumed with durable spill: %+v", sp)
	}
}

// TestDurableChangedSource: retained observation. Between the crash and the
// resume a key inside a reused certificate is deleted and one added there,
// and the same inside a gap: the certificate's rows are replayed as
// observed (the deleted key stays, the added one is missed), the gap is
// listed as it is now.
func TestDurableChangedSource(t *testing.T) {
	datasettest.RequireJournal(t)
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
	c.Start(1000, 0, nil)
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
	datasettest.RequireJournal(t)
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
	datasettest.RequireJournal(t)
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
	datasettest.RequireJournal(t)
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
	datasettest.RequireJournal(t)
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
	if len(b) < 16 || string(b[len(b)-8:]) != "SWATHDS2" {
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
	datasettest.RequireJournal(t)
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
	datasettest.RequireJournal(t)
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
	datasettest.RequireJournal(t)
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
