// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/binary"
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
			if q.Get("continuation-token") == "" && start < slow {
				time.Sleep(50 * time.Millisecond)
			}
		}
		time.Sleep(time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	return url, b, log
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
		cold.Resources.Spill.Durable.SealedFiles, d.Resume.Accepted, d.Resume.ReplayedRows, d.Resume.ReplaySkippedRows, d.Resume.Gaps, log.n.Load(), sum.Output.RowsListed, sum.Output.RowsRetained)
	if want := sum.Output.RowsListed; want <= d.Resume.ReplayedRows {
		t.Fatalf("listed %d rows, replayed %d", want, d.Resume.ReplayedRows)
	}
}

// crashDurable runs cfg until the crash point's nth occurrence (an
// injected crash, a kill's state) and checks it happened.
func crashDurable(t *testing.T, cfg Config, point string, nth int64) {
	t.Helper()
	var seen atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == point && seen.Add(1) == nth {
			return faults.ErrCrash
		}
		return nil
	})
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
	ents, _ := os.ReadDir(filepath.Join(out, order.DurableDir))
	var names []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".spill") {
			names = append(names, e.Name())
		}
	}
	return names
}

// TestDurableRepeatedRecovery (Sol 1): ledger frames persisted out of order
// (an interior record lost, later ones intact), a recorded file missing and
// an unrecorded file present; the resume is killed at each recovery step,
// twice, before one completes: the part journal is never refused, and the
// final dataset is the uninterrupted one.
func TestDurableRepeatedRecovery(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	ledger := filepath.Join(out, order.DurableDir, "ledger")
	b, err := os.ReadFile(ledger)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int
	for off := 0; off+8 <= len(b); off += 8 + int(uint32(b[off])|uint32(b[off+1])<<8|uint32(b[off+2])<<16|uint32(b[off+3])<<24) {
		offs = append(offs, off)
	}
	if len(offs) > 3 {
		for i := offs[len(offs)/2]; i < offs[len(offs)/2+1]; i++ {
			b[i] = 0
		}
		os.WriteFile(ledger, b, 0o644)
	}
	files := durableFiles(t, out)
	if len(files) < 2 {
		t.Fatalf("%d durable files after the crash", len(files))
	}
	os.Remove(filepath.Join(out, order.DurableDir, files[0]))
	os.WriteFile(filepath.Join(out, order.DurableDir, "0123456789abcdef0123456789abcdef.spill"), []byte("orphan"), 0o644)
	completed := false
	for _, point := range faults.RecoveryPoints {
		for range 2 {
			if completed {
				break
			}
			var seen atomic.Int64
			restore := faults.Set(func(p string) error {
				if p == point && seen.Add(1) == 1 {
					return faults.ErrCrash
				}
				return nil
			})
			_, err := Run(context.Background(), durableConfig(url, out, true))
			restore()
			if seen.Load() > 0 && !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("%s: %v", point, err)
			}
			if seen.Load() == 0 {
				t.Logf("%s not reached (nothing to do there): the resume completed (%v)", point, err)
				completed = err == nil
				if err != nil {
					t.Fatal(err)
				}
			}
			if errors.Is(err, journal.ErrCorrupt) {
				t.Fatalf("%s: the part journal refused: %v", point, err)
			}
		}
	}
	if !completed {
		sum, err := Run(context.Background(), durableConfig(url, out, true))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("final resume: %+v", sum.Resources.Spill.Durable.Resume)
	}
	sameDataset(t, "repeated recovery", ref, out)
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

// TestDurableCompressionFlip: a resume with --spill-compress flipped replays
// the zstd frames all the same.
func TestDurableCompressionFlip(t *testing.T) {
	url, _, ref, out := durableFixture(t)
	cfg := durableConfig(url, out, true)
	cfg.Limits.Spill.Uncompressed = true
	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "compression flipped", ref, out)
	if sum.Resources.Spill.Durable.Resume.ReplayedRows == 0 {
		t.Fatal("nothing replayed")
	}
}

// TestDurableVersions: a version 7 journal resumes with durable spill off,
// writing nothing of version 8; a version 9 journal is refused before
// recovery changes anything.
func TestDurableVersions(t *testing.T) {
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
	c, err := order.RecoverDurable(root, nil, st.Header.RunUUID, st.Header.Identity.Hash(), k)
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
