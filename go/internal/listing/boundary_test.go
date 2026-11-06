// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/s3"
)

// scripted is a resumable test source over a fixed key list, split into
// ranges of chunk keys (the root emits each range as a child), each range
// ascending on its own: a broken list (a range starting at or below the
// previous range's last key) is a provider that breaks the ordering
// contract exactly at a range boundary.
type scripted struct {
	keys   []string
	chunk  int
	after  *string
	listed atomic.Int64
	slow   time.Duration // before every range after the first: lets parts publish early
	big    string        // this key's row is larger than any part: a part boundary follows it
}

type scriptedRange struct{ i int } // -1: the root; else the range starting at key i

func (s *scripted) Root() scriptedRange { return scriptedRange{-1} }

func (s *scripted) Expand(ctx context.Context, r scriptedRange, e source.Emitter[scriptedRange, s3.Object]) error {
	if r.i < 0 {
		for i := 0; i < len(s.keys); i += s.chunk {
			if err := e.Child(scriptedRange{i}); err != nil {
				return err
			}
		}
		return nil
	}
	if r.i > 0 && s.slow > 0 {
		time.Sleep(s.slow)
	}
	var rows []schema.Row[s3.Object]
	for _, k := range s.keys[r.i:min(r.i+s.chunk, len(s.keys))] {
		if s.after != nil && k <= *s.after {
			continue
		}
		o := s3.Object{Size: int64(len(k)), ETag: "e" + k}
		if k == s.big {
			o.ETag = strings.Repeat("x", 256<<10)
		}
		rows = append(rows, schema.Row[s3.Object]{Key: k, Data: o})
	}
	if len(rows) == 0 {
		return nil
	}
	err := e.Rows(rows)
	s.listed.Add(int64(len(rows)))
	return err
}

type scriptedSource struct {
	registry.Bound[s3.Object]
	p *scripted
}

func (o *scriptedSource) Locator() source.Locator {
	return source.Locator{Kind: "scripted", Root: "scripted://x"}
}
func (o *scriptedSource) Listed() int64                     { return o.p.listed.Load() }
func (o *scriptedSource) Restart(after string) error        { o.p.after = &after; return nil }
func (o *scriptedSource) Capabilities() source.Capabilities { return source.Capabilities{Resume: true} }

var (
	scriptMu   sync.Mutex
	scriptNext *scripted
)

func init() {
	registry.Register(registry.Factory{Scheme: "scripted", Defaults: func() any { return nil },
		Open: func(ctx context.Context, req registry.Request) (registry.Source, error) {
			scriptMu.Lock()
			defer scriptMu.Unlock()
			p := scriptNext
			return &scriptedSource{Bound: registry.Bind[scriptedRange, s3.Object](p, &s3.Schema), p: p}, nil
		}})
}

func runScripted(t *testing.T, p *scripted, out string, resume bool) (Report, error) {
	t.Helper()
	scriptMu.Lock()
	scriptNext = p
	scriptMu.Unlock()
	return Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: resume},
		Limits: Limits{PartSize: 2 << 10, Workers: 4, Memory: 256 << 10}})
}

// TestBoundaryDefectNeverReachesTheJournal: a provider whose ranges are
// each ascending but whose boundary keys repeat or descend, with parts
// published early before the bad boundary. The run fails with an invariant
// error; every recorded part chains (the journal reads back, the parts
// before the defect are a valid prefix); after the provider is fixed, the
// resume completes a dataset identical to a clean run's. Before the guard,
// the bad boundary was routed into parts, published and journaled, and only
// the final gate refused the manifest: the journal then named overlapping
// parts and a resume continued after them.
func TestBoundaryDefectNeverReachesTheJournal(t *testing.T) {
	var keys []string
	for i := range 40000 {
		keys = append(keys, fmt.Sprintf("k%06d", i))
	}
	const chunk = 2000
	// The range starting at key b begins at or below key b-1, and a part
	// boundary falls exactly between them (key b-1's row fills a part), so
	// each part is ascending on its own: only adjacency can see the defect.
	b := 30000
	big := keys[b-1]
	clean := t.TempDir()
	if _, err := runScripted(t, &scripted{keys: keys, chunk: chunk, big: big}, clean, false); err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"repeat", "descent"} {
		t.Run(defect, func(t *testing.T) {
			bad := append([]string(nil), keys...)
			if defect == "repeat" {
				bad[b] = bad[b-1]
			} else {
				bad[b] = bad[b-2]
			}
			if !sort.StringsAreSorted(bad[b : b+chunk]) {
				t.Fatal("the range itself must stay ascending")
			}
			out := t.TempDir()
			// The defective run, then a resume against the still broken
			// provider (the resumed seam, after the recorded prefix).
			for attempt, resume := range []bool{false, true} {
				_, err := runScripted(t, &scripted{keys: bad, chunk: chunk, slow: 10 * time.Millisecond, big: big}, out, resume)
				var ie *dataset.InvariantError
				if !errors.As(err, &ie) {
					t.Fatalf("attempt %d: err = %v, want an invariant error", attempt, err)
				}
				st, rerr := journal.Read(out)
				if rerr != nil {
					t.Fatalf("attempt %d: the journal does not read back after the defect: %v", attempt, rerr)
				}
				if len(st.Records) == 0 {
					t.Fatal("no part was published before the defect: the test does not exercise early publication")
				}
				if last := st.Last(); string(last.Part.MaxKey) > big {
					t.Fatalf("attempt %d: a recorded part reaches the defect: max %q", attempt, last.Part.MaxKey)
				}
				if _, err := os.Stat(filepath.Join(out, dataset.ManifestName)); err == nil {
					t.Fatal("a manifest was published")
				}
			}
			// The provider is fixed: the resume completes the dataset.
			if _, err := runScripted(t, &scripted{keys: keys, chunk: chunk, big: big}, out, true); err != nil {
				t.Fatal(err)
			}
			sameDataset(t, defect, clean, out)
		})
	}
}
