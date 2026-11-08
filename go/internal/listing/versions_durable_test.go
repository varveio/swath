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
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// hotVersions is a versioned bucket with hot keys for durable spill: 4,000
// keys, every 37th with 40 versions (a group spanning several 7-entry
// pages, so page boundaries fall inside groups), the rest 1 to 3; a slow
// frontier (fresh requests below the 10 % key wait), so the ranges ahead
// spill and seal certificates. It logs the key-marker of each fresh data
// request (no version-id-marker, no delimiter) while logging is on: with
// versions the key-marker is the request's exclusive start, as start-after
// is for ListObjectsV2.
type hotVersions struct {
	rows    []client.Version
	slow    string
	sealed  atomic.Int64 // durable seals so far (order's "sealed" events)
	held    atomic.Int64 // fresh requests from the scope's start this run
	logging atomic.Bool
	mu      sync.Mutex
	starts  []string
}

func newHotVersions() *hotVersions {
	p := &hotVersions{}
	for i := range 4000 {
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
	p.slow = "k0400"
	return p
}

func (p *hotVersions) Route(string) string { return "https://versions.invalid/b" }
func (p *hotVersions) ListObjectsV2(context.Context, *client.ListInput) (*client.Page, error) {
	return nil, errors.New("hotVersions lists versions only")
}

func (p *hotVersions) ListObjectVersions(_ context.Context, in *client.VersionsInput) (*client.VersionPage, error) {
	if in.Delimiter == "" && in.VersionIDMarker == "" && in.KeyMarker != "" && in.KeyMarker < "k0010" && p.held.Add(1) == 1 {
		// The frontier's second page (in group mode each page starts fresh
		// after the last complete key) waits until the ranges ahead have
		// sealed eight certificates (or 5 s): K stays at the bottom while
		// they seal, so the kills find certificates live above it.
		for deadline := time.Now().Add(5 * time.Second); p.sealed.Load() < 8 && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
	}
	if in.Delimiter == "" && in.VersionIDMarker == "" {
		if p.logging.Load() {
			p.mu.Lock()
			p.starts = append(p.starts, in.KeyMarker)
			p.mu.Unlock()
		}
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
	api.held.Store(0)
	api.sealed.Store(0)
	return Config{Source: SourceConfig{URI: "s3://b", Options: &s3.Options{Client: api, AllVersions: true, Policy: keyrange.Policy{PageSize: 7}}},
		Output: OutputConfig{Dir: out, Resume: resume},
		Limits: Limits{Workers: 16, PartSize: 2 << 10, RowBudget: minRowBudget, Spill: SpillLimits{Max: 64 << 20}, PartInterval: -1}}
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
	b, err := os.ReadFile(filepath.Join(out, order.DurableDir, "ledger"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		t.Fatal(err)
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
	return certs
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
	api := newHotVersions()
	defer order.SetDurableEvents(func(e string) {
		if e == "sealed" {
			api.sealed.Add(1)
		}
	})()
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
	} {
		name := kill.point
		if kill.resumeKill != "" {
			name += "+" + kill.resumeKill
		}
		t.Run(name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			crashDurable(t, hotVersionsConfig(api, out, false), kill.point, kill.nth)
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
			for _, s := range api.starts {
				for _, c := range certs {
					if (c.open || c.ce > k) && s >= max(c.cs, k) && (c.open || s < c.ce) {
						t.Errorf("a data request starts at %q inside the certificate (%q, %q] (K %q)", s, c.cs, c.ce, k)
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

