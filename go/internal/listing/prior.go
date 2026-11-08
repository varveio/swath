// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
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
}

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
	if cfg.Output.Dir != "" && samePath(path, cfg.Output.Dir) {
		return nil, bad("the prior is the output directory (write each refresh to a new directory)")
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, bad("%v", err)
	}
	var pr *source.Prior
	if fi.IsDir() {
		pr, err = datasetPrior(path, src.Locator())
	} else {
		pr, err = cutFilePrior(path)
	}
	if err != nil {
		return nil, bad("%v", err)
	}
	if err := seeded.SeedFrom(pr); err != nil {
		return nil, fmt.Errorf("--seed-from %s: %w", path, err)
	}
	return &Prior{Path: path, Source: pr.Source, Cuts: len(pr.Cuts), Counted: pr.Counted}, nil
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

// datasetPrior reads a committed dataset's distribution (this build's
// SeekParquet format, or an earlier build's format 2): its
// identity is the source kind and container (the bucket) of the manifest,
// which must be the run's, and its scope, which the engine checks against
// the run's prefix (Prior.Check); an endpoint or region is not in the
// manifest and is not checked. Its cuts are priorCuts keys of about equal
// rows: a cut falls at the end of the key page (1,024 rows) where the
// running row count reaches the next multiple of rows/priorCuts, read from
// the page index of the parts such a multiple falls in (KeyPages); a part
// whose index cannot be read is cut at its own last key. The dataset's
// last key always closes the last cut.
func datasetPrior(dir string, loc source.Locator) (*source.Prior, error) {
	if !dataset.Committed(dir) {
		return nil, errors.New("not a committed dataset (no manifest.json)")
	}
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil {
		return nil, err
	}
	var m dataset.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	switch {
	case !m.Readable():
		return nil, fmt.Errorf("manifest format %s %d, want %s %d (draft %d) or %s %d", m.Format, m.FormatVersion,
			seekparquet.Format, seekparquet.FormatVersion, seekparquet.DraftRevision, dataset.LegacyFormat, dataset.LegacyFormatVersion)
	case m.Source.Kind != loc.Kind || m.Source.Container != loc.Container:
		return nil, fmt.Errorf("the dataset lists %s (%s %q), the run %s (%s %q)", m.Source.URI, m.Source.Kind, m.Source.Container, loc.Root, loc.Kind, loc.Container)
	}
	pr := &source.Prior{Source: "dataset", Scope: m.Scope.Prefix, Counted: true, Parts: len(m.Parts)}
	var total int64
	for i, p := range m.Parts {
		if p.Rows <= 0 || (i > 0 && p.MinKey <= m.Parts[i-1].MaxKey) {
			return nil, fmt.Errorf("manifest part %d: empty, or not after part %d", i, i-1)
		}
		total += p.Rows
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
		pages, err := parquetout.KeyPages(filepath.Join(dir, dataset.DataDir, dataset.PartName(i)), !m.Current())
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
	return pr, nil
}

// maxCutFileBytes and maxCutFileCuts bound a cut file. Its cuts are held
// for the whole run, and the engine merges them into at most
// keyrange.Tiles tiles (1,000), so more cuts add no precision, only
// memory: a file past either bound is refused before any request instead
// of read. 16 MiB of lines and 262,144 cuts hold about 30 MiB at most
// (the keys, and 24 bytes per cut twice: the prior's and the tiling's
// candidates); a run's row budget defaults to 128 MiB.
const (
	maxCutFileBytes = 16 << 20
	maxCutFileCuts  = 1 << 18
)

// cutFilePrior reads a cut file: one cut key per line (LF), ascending
// bytewise without repeats, each optionally followed by a tab and the
// count of keys expected in the interval it closes (after the previous
// cut); every line has a count or none does. A key that itself contains a
// tab must carry a count (the count is after the last tab). A cut file
// records no scope: the run's is assumed, and every cut must lie inside it
// (source.Prior.Check).
func cutFilePrior(path string) (*source.Prior, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil {
		return nil, err
	} else if fi.Size() > maxCutFileBytes {
		return nil, fmt.Errorf("the cut file is %d bytes, more than %d: keep fewer cuts (every nth line, its count the sum of the lines it replaces)", fi.Size(), maxCutFileBytes)
	}
	pr := &source.Prior{Source: "cuts", Unscoped: true}
	// The stat refuses a large regular file before reading; the budget
	// holds while scanning, for an input whose size the stat does not
	// tell (a pipe, a process substitution) or a file that grows.
	in := &io.LimitedReader{R: f, N: maxCutFileBytes + 1}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64<<10), 64<<10)
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
		if len(pr.Cuts) == maxCutFileCuts {
			return nil, fmt.Errorf("the cut file holds more than %d cuts: keep fewer (every nth line, its count the sum of the lines it replaces)", maxCutFileCuts)
		}
		pr.Cuts = append(pr.Cuts, c)
	}
	if in.N == 0 {
		return nil, fmt.Errorf("the cut file holds more than %d bytes: keep fewer cuts (every nth line, its count the sum of the lines it replaces)", maxCutFileBytes)
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
