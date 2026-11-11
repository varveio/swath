// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"bufio"
	"fmt"
	"math"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/varveio/swath/internal/source/keyrange"
)

// pageOracleEnv names the experiment's page oracle (#449, not a product
// feature): PATH[:MODE], PATH a replay TSV fixture of the listed
// container's keys. Before each range request the oracle counts the keys
// left in the range, n, and asks for n+2 (the seek's own key, which an
// exclusive start lists, and the first key past the range, which ends its
// span), so a range ends in its last read with at most two rows past it:
//
//	grow: min(n+2, the provider's maximum): the fewest requests any page
//	      policy can make, with almost no rows past a range's end;
//	trim: min(n+2, the nominal page size): no growth, almost no rows past
//	      a range's end (what an exact end bound would give at 1,000).
//
// The adapters read once per page (order.Adapter.OneReadPerPage), so no
// read's surplus is carried into the counted read. It bounds what a page
// policy could gain; it is never a policy.
const pageOracleEnv = "SWATH_EXP_PAGE_ORACLE"

// pageLogEnv names the shadow mode's log file (pageOracle.shadow).
const pageLogEnv = "SWATH_EXP_PAGE_LOG"

type pageOracle struct {
	keys   []string // byte-sorted
	prefix string
	grow   bool
	// shadow (mode shadow): the oracle sizes nothing; each range request
	// of the run's own policy is logged here with the keys truly left, one
	// JSON line (the engine's estimate against the truth).
	shadow *os.File
	mu     sync.Mutex
}

// logged is the policy inner (nil: the nominal size) logging each request
// to the shadow file.
func (o *pageOracle) logged(inner func(keyrange.PageHint) int) func(keyrange.PageHint) int {
	return func(h keyrange.PageHint) int {
		ask := h.Nominal
		if inner != nil {
			ask = inner(h)
		}
		rem := "null"
		if !math.IsNaN(h.Rem) && !math.IsInf(h.Rem, 0) {
			rem = strconv.FormatFloat(h.Rem, 'f', 0, 64)
		} else if math.IsInf(h.Rem, 1) {
			rem = `"inf"`
		}
		line := fmt.Sprintf(`{"first":%v,"open":%v,"rem":%s,"keys":%d,"full":%d,"asked":%d,"got":%d,"ask":%d,"left":%d}`+"\n",
			h.First, h.Open, rem, h.Keys, h.Full, h.Asked, h.Got, ask, o.left(h.Cursor, h.Hi, h.Open))
		o.mu.Lock()
		o.shadow.WriteString(line)
		o.mu.Unlock()
		return ask
	}
}

// loadPageOracle reads the oracle the environment names, if any.
func loadPageOracle(prefix string) (*pageOracle, error) {
	spec := os.Getenv(pageOracleEnv)
	if spec == "" {
		return nil, nil
	}
	path, mode := spec, "grow"
	if i := strings.LastIndexByte(spec, ':'); i >= 0 {
		path, mode = spec[:i], spec[i+1:]
	}
	if mode != "grow" && mode != "trim" && mode != "shadow" {
		return nil, fmt.Errorf("%s: mode %q, want grow, trim or shadow", pageOracleEnv, mode)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", pageOracleEnv, err)
	}
	defer f.Close()
	o := &pageOracle{prefix: prefix, grow: mode == "grow"}
	if mode == "shadow" {
		if o.shadow, err = os.Create(os.Getenv(pageLogEnv)); err != nil {
			return nil, fmt.Errorf("%s: %w", pageLogEnv, err)
		}
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		k, _, _ := strings.Cut(line, "\t")
		if k, err = url.PathUnescape(k); err != nil {
			return nil, fmt.Errorf("%s: %w", pageOracleEnv, err)
		}
		if strings.HasPrefix(k, prefix) {
			o.keys = append(o.keys, k)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", pageOracleEnv, err)
	}
	slices.Sort(o.keys)
	return o, nil
}

// left is the number of keys in (cursor, hi], or above cursor when open.
func (o *pageOracle) left(cursor, hi string, open bool) int {
	from := sort.Search(len(o.keys), func(i int) bool { return o.keys[i] > cursor })
	to := len(o.keys)
	if !open {
		to = sort.Search(len(o.keys), func(i int) bool { return o.keys[i] > hi })
	}
	return max(0, to-from)
}

// pageFor is the oracle's keyrange.Options.PageFor; max is the provider's
// largest page.
func (o *pageOracle) pageFor(maxPage int) func(keyrange.PageHint) int {
	return func(h keyrange.PageHint) int {
		n := o.left(h.Cursor, h.Hi, h.Open) + 2
		if o.grow {
			return min(n, maxPage)
		}
		return min(n, h.Nominal)
	}
}
