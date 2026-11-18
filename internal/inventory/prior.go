// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package inventory

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
)

// ErrPrior marks a --seed-from that cannot seed the run: a refusal, nothing
// is listed.
var ErrPrior = source.ErrPrior

// Prior is the run report's account of a --seed-from prior.
type Prior struct {
	Path    string `json:"path"`
	Source  string `json:"source"` // "dataset" or "cuts"
	Cuts    int    `json:"cuts"`   // cut keys offered, before the merge into tiles
	Counted bool   `json:"counted"`
	// CutsFile, for a dataset: how its cut file (dataset.CutsName) was
	// taken: "read" (its cuts are the prior), "absent" or "refused"
	// (CutsRefused says why); the key page index is read without it.
	CutsFile    string `json:"cuts_file,omitempty"`
	CutsRefused string `json:"cuts_refused,omitempty"`
}

// cutsFile is how a dataset prior took its cut file (Prior.CutsFile and
// Prior.CutsRefused).
type cutsFile struct{ state, refused string }

// priorCuts is how many cuts a dataset prior offers: 4,096, four times
// the most tiles a range-engine run starts with (1,000), so the
// engine's merge into tiles of equal mass picks each tile's end among at
// least four cuts, and no part's page index is read unless a cut lies in
// it.
const priorCuts = 4096

// seedFrom gives src the prior at path, before Root (source.Seeded).
func seedFrom(cfg Config, src registry.Source) (*Prior, error) {
	path := cfg.Source.SeedFrom
	bad := func(f string, a ...any) error {
		return fmt.Errorf("%w: --seed-from %s: %s", ErrPrior, path, fmt.Sprintf(f, a...))
	}
	seeded, ok := src.(source.Seeded)
	if !ok {
		return nil, bad("the %s source does not list in key ranges", src.Locator().Kind)
	}
	if out := cfg.Output.final(); out != "" && samePath(path, out) {
		return nil, bad("the prior is the output directory (write each refresh to a new directory)")
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, bad("%v", err)
	}
	var pr *source.Prior
	var side cutsFile
	if fi.IsDir() {
		// Without its cut file, a run that orders its rows plans its tiles
		// from the whole page index as it goes; a dry run (order.Count,
		// which takes no plan) tiles at once from cuts sampled from it.
		pr, side, err = datasetPrior(path, src.Locator(), !dryRun(cfg), true)
	} else {
		pr, err = cutFilePrior(path)
	}
	if err != nil {
		return nil, bad("%v", err)
	}
	err = seeded.SeedFrom(pr)
	if err != nil && side.state == "read" {
		// The engine refused the cut file's cuts (a key it cannot list, as
		// the dataset's own keys can): its page index is read instead.
		side = cutsFile{state: "refused", refused: err.Error()}
		if pr, _, err = datasetPrior(path, src.Locator(), !dryRun(cfg), false); err != nil {
			return nil, bad("%v", err)
		}
		err = seeded.SeedFrom(pr)
	}
	if err != nil {
		return nil, fmt.Errorf("--seed-from %s: %w", path, err)
	}
	if side.refused != "" && cfg.Warn != nil {
		cfg.Warn(source.WarnSeedFromCuts, fmt.Sprintf("--seed-from %s: its %s is not used (%s): its key page index is read instead", path, dataset.CutsName, side.refused))
	}
	return &Prior{Path: path, Source: pr.Source, Cuts: len(pr.Cuts), Counted: pr.Counted, CutsFile: side.state, CutsRefused: side.refused}, nil
}

// samePath: a and b name the same directory (both exist and are one
// file, or their absolute paths are equal).
func samePath(a, b string) bool {
	fa, ea := os.Stat(a)
	fb, eb := os.Stat(b)
	if ea == nil && eb == nil {
		return os.SameFile(fa, fb)
	}
	aa, _ := filepath.Abs(a)
	ab, _ := filepath.Abs(b)
	return aa == ab
}

// datasetPrior reads a committed SeekParquet dataset's distribution: its
// identity is the source kind and container (the bucket) of the manifest,
// which must be the run's, and its scope, which the engine checks against
// the run's prefix (Prior.Check); an endpoint or region is not in the
// manifest and is not checked. With cuts, its cuts are its cut file's when
// it holds one that is its own (datasetCuts). Else, with stream, they are every key
// page's, read as the run goes (pageStream); without, priorCuts keys of
// about equal rows: a cut falls at the end of the key page (1,024 rows)
// where the running row count reaches the next multiple of rows/priorCuts,
// read from the page index of the parts such a multiple falls in
// (KeyPages); a part whose index cannot be read is cut at its own last
// key. The dataset's last key always closes the last cut.
func datasetPrior(dir string, loc source.Locator, stream, cuts bool) (*source.Prior, cutsFile, error) {
	var side cutsFile
	if !dataset.Committed(dir) {
		return nil, side, errors.New("not a committed dataset (no manifest.json)")
	}
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil {
		return nil, side, err
	}
	var m dataset.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, side, fmt.Errorf("manifest: %w", err)
	}
	switch {
	case !m.Current():
		return nil, side, fmt.Errorf("manifest format %s %d draft %d, want %s %d draft %d", m.Format, m.FormatVersion, m.DraftRevision,
			seekparquet.Format, seekparquet.FormatVersion, seekparquet.DraftRevision)
	case m.Source.Kind != loc.Kind || m.Source.Container != loc.Container:
		// An older manifest may hold a credential in its root's authority,
		// which is also its container.
		shown := func(container string) string {
			if strings.ContainsAny(container, "@?#") {
				return "[redacted]"
			}
			return container
		}
		return nil, side, fmt.Errorf("the dataset lists %s (%s %q), the run %s (%s %q)", registry.Redact(m.Source.URI), m.Source.Kind, shown(m.Source.Container), registry.Redact(loc.Root), loc.Kind, shown(loc.Container))
	}
	pr := &source.Prior{Source: "dataset", Scope: m.Scope.Prefix, Counted: true, Parts: len(m.Parts)}
	var total int64
	for i, p := range m.Parts {
		if p.Rows <= 0 || (i > 0 && p.MinKey <= m.Parts[i-1].MaxKey) {
			return nil, side, fmt.Errorf("manifest part %d: empty, or not after part %d", i, i-1)
		}
		total += p.Rows
		pr.Bytes += p.Bytes
	}
	pr.Rows = total
	if cuts {
		switch got, err := datasetCuts(dir, &m, total); {
		case err != nil:
			side = cutsFile{state: "refused", refused: err.Error()}
		case got == nil:
			side.state = "absent"
		default:
			pr.Cuts = got
			return pr, cutsFile{state: "read"}, nil
		}
	}
	if stream {
		ps := &pageStream{dir: dir}
		for _, p := range m.Parts {
			ps.parts = append(ps.parts, streamPart{min: string(p.MinKey), max: string(p.MaxKey), rows: p.Rows})
		}
		pr.Stream = ps
		return pr, side, nil
	}
	// threshold j is the row count the j-th cut should reach.
	threshold := func(j int64) int64 { return (j*total + priorCuts - 1) / priorCuts }
	var cum, last int64 // rows through the latest page; through the latest cut
	j := int64(1)
	cut := func(key string) {
		n := len(pr.Cuts)
		if n > 0 && key < pr.Cuts[n-1].Key {
			return
		}
		if n > 0 && key == pr.Cuts[n-1].Key {
			pr.Cuts[n-1].Rows += cum - last
		} else {
			pr.Cuts = append(pr.Cuts, source.Cut{Key: key, Rows: cum - last})
		}
		last = cum
		for j < priorCuts && threshold(j) <= cum {
			j++
		}
	}
	for i, p := range m.Parts {
		end := cum + p.Rows
		if j >= priorCuts || threshold(j) > end {
			cum = end // no cut inside this part
			continue
		}
		pages, err := parquetout.KeyPages(filepath.Join(dir, dataset.DataDir, dataset.PartName(i)))
		var sum int64
		for _, pg := range pages {
			sum += pg.Rows
		}
		if err != nil || sum != p.Rows || len(pages) == 0 || string(pages[len(pages)-1].Max) != string(p.MaxKey) {
			pr.IndexFailed++
			cum = end
			cut(string(p.MaxKey))
			continue
		}
		pr.IndexedParts++
		for _, pg := range pages {
			cum += pg.Rows
			if j < priorCuts && cum >= threshold(j) {
				cut(string(pg.Max))
			}
		}
	}
	if n := len(m.Parts); n > 0 && cum > last {
		cut(string(m.Parts[n-1].MaxKey))
	}
	return pr, side, nil
}

// datasetCuts reads the cut file a dataset holds (dataset.CutsName) as
// its prior's cuts when it is the dataset's own: every line counted, the
// cuts ascending without repeats from the first part's first key, the last
// the dataset's last key, each count positive and the counts summing to
// the manifest's rows (never past them on the way, so no sum wraps). nil
// without an error: the dataset holds none (or no rows).
func datasetCuts(dir string, m *dataset.Manifest, rows int64) ([]source.Cut, error) {
	n := len(m.Parts)
	path := filepath.Join(dir, dataset.CutsName)
	if _, err := os.Lstat(path); n == 0 || errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	pr, err := cutFilePrior(path)
	if err != nil {
		return nil, err
	}
	if !pr.Counted {
		return nil, errors.New("its lines carry no counts")
	}
	var sum int64
	for i, c := range pr.Cuts {
		switch {
		case i == 0 && c.Key < string(m.Parts[0].MinKey):
			return nil, fmt.Errorf("its first cut %.64q is before the dataset's first key", c.Key)
		case i > 0 && c.Key <= pr.Cuts[i-1].Key:
			return nil, fmt.Errorf("cut %d %.64q does not sort after cut %d", i+1, c.Key, i)
		case c.Rows <= 0 || c.Rows > rows-sum:
			return nil, fmt.Errorf("cut %d counts %d rows, after %d of the manifest's %d", i+1, c.Rows, sum, rows)
		}
		sum += c.Rows
	}
	switch last := pr.Cuts[len(pr.Cuts)-1].Key; {
	case last != string(m.Parts[n-1].MaxKey):
		return nil, fmt.Errorf("it ends at %.64q, the dataset at %.64q", last, m.Parts[n-1].MaxKey)
	case sum != rows:
		return nil, fmt.Errorf("its counts sum to %d rows, the manifest's to %d", sum, rows)
	}
	return pr.Cuts, nil
}

// cutFilePrior reads a cut file: one cut key per line (LF), ascending
// bytewise without repeats, each optionally followed by a tab and the
// count of keys expected in the interval it closes (after the previous
// cut); every line has a count or none does. A key that itself contains a
// tab must carry a count (the count is after the last tab). A cut file
// records no scope: the run's is assumed, and every cut must lie inside it
// (source.Prior.Check). Its cuts are held for the whole run, and a dry run
// merges them into at most keyrange.Tiles tiles (1,000), so more cuts add
// no precision, only memory: a file of more than dataset.MaxCutsBytes or
// dataset.MaxCuts cuts is refused before any request instead of read.
// 16 MiB of lines and 262,144 cuts hold about 30 MiB at most (the keys,
// and 24 bytes per cut twice: the prior's and the tiling's candidates); a
// run's row budget is at least 128 KiB, by default 1/16 of memory
// (rowBudgetFor).
func cutFilePrior(path string) (*source.Prior, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil {
		return nil, err
	} else if fi.Size() > dataset.MaxCutsBytes {
		return nil, fmt.Errorf("the cut file is %d bytes, more than %d: keep fewer cuts (every nth line, its count the sum of the lines it replaces)", fi.Size(), dataset.MaxCutsBytes)
	}
	pr := &source.Prior{Source: "cuts", Unscoped: true}
	// The stat refuses a large regular file before reading; the budget
	// holds while scanning, for an input whose size the stat does not
	// tell (a pipe, a process substitution) or a file that grows.
	in := &io.LimitedReader{R: f, N: dataset.MaxCutsBytes + 1}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, dataset.MaxCutLine), dataset.MaxCutLine)
	counted := 0
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		if line == "" {
			return nil, fmt.Errorf("line %d is empty", n)
		}
		c := source.Cut{Key: line}
		if i := strings.LastIndexByte(line, '\t'); i >= 0 {
			if v, err := strconv.ParseInt(line[i+1:], 10, 64); err == nil && v >= 0 {
				c, counted = source.Cut{Key: line[:i], Rows: v}, counted+1
			}
		}
		if len(pr.Cuts) == dataset.MaxCuts {
			return nil, fmt.Errorf("the cut file holds more than %d cuts: keep fewer (every nth line, its count the sum of the lines it replaces)", dataset.MaxCuts)
		}
		pr.Cuts = append(pr.Cuts, c)
	}
	if in.N == 0 {
		return nil, fmt.Errorf("the cut file holds more than %d bytes: keep fewer cuts (every nth line, its count the sum of the lines it replaces)", dataset.MaxCutsBytes)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	switch {
	case len(pr.Cuts) == 0:
		return nil, errors.New("the cut file holds no cut")
	case counted != 0 && counted != len(pr.Cuts):
		return nil, fmt.Errorf("%d of %d lines carry a count: every line has one or none does", counted, len(pr.Cuts))
	}
	pr.Counted = counted > 0
	return pr, nil
}

// pageStream is a dataset prior's key page index read as the run goes
// (source.CutStream): one goroutine reads the parts' indexes in key order,
// from the first part that ends after the resume key, each part one batch
// of cuts (a page's last key and rows), at most streamAhead batches ahead
// of the plan. A part whose index cannot be read, or does not match the
// manifest (its rows, its last key, its pages' keys ascending inside the
// part's bounds), is one cut at its last key with its rows, as datasetPrior
// does.
type pageStream struct {
	dir   string
	parts []streamPart
	ch    chan *source.CutBatch
	end   error // io.EOF, or the cause the read-ahead stopped for (set before ch closes)
}

// streamPart is a manifest part as the stream reads it.
type streamPart struct {
	min, max string
	rows     int64
}

// streamAhead is the batches (parts) read ahead of the plan.
const streamAhead = 2

func (s *pageStream) Start(ctx context.Context, after string, wake func()) func() {
	s.ch = make(chan *source.CutBatch, streamAhead)
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	first := sort.Search(len(s.parts), func(i int) bool { return s.parts[i].max > after })
	go func() {
		defer close(done)
		defer wake()
		defer close(s.ch)
		for i := first; i < len(s.parts); i++ {
			if err := faults.At("seed-from-part"); err != nil { // tests: the prior's reader fails mid-run
				s.end = err
				return
			}
			select {
			case s.ch <- s.part(i):
				wake()
			case <-ctx.Done():
				s.end = context.Cause(ctx)
				return
			}
		}
		s.end = io.EOF
	}()
	return func() {
		cancel(errors.New("--seed-from: the plan stopped"))
		<-done
	}
}

func (s *pageStream) Next() (*source.CutBatch, error) {
	select {
	case b, ok := <-s.ch:
		if !ok {
			return nil, s.end
		}
		return b, nil
	default:
		return nil, nil
	}
}

// part is part i's batch: its key pages, or its own last key.
func (s *pageStream) part(i int) *source.CutBatch {
	p := s.parts[i]
	pages, err := parquetout.KeyPages(filepath.Join(s.dir, dataset.DataDir, dataset.PartName(i)))
	var sum int64
	prev := p.min
	for _, pg := range pages {
		k := string(pg.Max)
		if k < prev || k > p.max {
			err = errors.New("key pages out of order")
		}
		sum += pg.Rows
		prev = k
	}
	if err != nil || sum != p.rows || len(pages) == 0 || prev != p.max {
		return &source.CutBatch{Cuts: []source.Cut{{Key: p.max, Rows: p.rows}}, Part: true}
	}
	b := &source.CutBatch{Cuts: make([]source.Cut, len(pages)), Part: true, Indexed: true}
	for j, pg := range pages {
		b.Cuts[j] = source.Cut{Key: string(pg.Max), Rows: pg.Rows}
	}
	return b
}
