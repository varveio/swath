// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// A committed dataset carries its key distribution for the next listing's
// --seed-from beside its manifest: CutsName, a cut file (one cut a line:
// the key, a tab, and the rows after the previous cut through it). It is
// advisory and outside the SeekParquet format: the manifest does not name
// it, readers ignore it, and a dataset without it is whole.
const CutsName = "cuts.txt"

// MaxCutsBytes and MaxCuts bound a cut file: --seed-from refuses a larger
// one. MaxCutLine is the longest line it reads, the line feed included.
const (
	MaxCutsBytes = 16 << 20
	MaxCuts      = 1 << 18
	MaxCutLine   = 64 << 10
)

// PageEnd is where a part may be cut for the cut file: the end of a key
// page the writer filled (1,024 rows), with that page's last key and the
// part's rows through it. The end of a page whose next page starts with
// the same key (inside a version group) is not one; the part's last row
// always is.
type PageEnd struct {
	Key  string
	Rows int64
}

// Cuts selects a dataset's cuts from its parts' page ends, given in key
// order: with R rows a cut, a cut falls at the first page end where the
// dataset's rows reach each multiple of R, and the dataset's last key
// closes the last line. R is the least power of two for which the file
// holds at most MaxCuts lines and MaxCutsBytes bytes (1: every page end).
// The cuts at 2R are those at R where the rows reach a multiple of 2R since
// the previous cut at R, so the set coarsens in place: it doubles R as soon
// as the cuts kept so far exceed either bound (the file at that R would),
// and holds at most MaxCuts cuts and MaxCutsBytes bytes of lines between
// parts. A cut whose key cannot be a line (it holds a line feed, or is
// longer than MaxCutLine allows) is selected as any other but not written:
// its rows count toward the next line.
type Cuts struct {
	maxCuts  int
	maxBytes int64
	unit     int64    // R
	base     int64    // the rows before the first part
	rows     int64    // the rows through the last part
	last     string   // the last part's last key
	kept     []cutEnd // the cuts at R
	bytes    int64    // their lines' bytes, each counted as written
}

// cutEnd is a cut: its key and the dataset's rows through it.
type cutEnd struct {
	key  string
	rows int64
}

// NewCuts starts the cuts of the parts after the first base rows of a
// dataset.
func NewCuts(base int64) *Cuts {
	return &Cuts{maxCuts: MaxCuts, maxBytes: MaxCutsBytes, unit: 1, base: base, rows: base}
}

// Part adds the next part's page ends, the last one its last row.
func (c *Cuts) Part(ends []PageEnd) {
	base := c.rows
	for _, e := range ends {
		at := base + e.Rows
		if at/c.unit > c.rows/c.unit {
			c.bytes += lineBytes(e.Key, at-c.lastRows())
			c.kept = append(c.kept, cutEnd{key: e.Key, rows: at})
			for len(c.kept) > c.maxCuts || c.bytes > c.maxBytes {
				c.coarsen()
			}
		}
		c.rows, c.last = at, e.Key
	}
}

// Join appends the cuts of the parts that follow c's (next started where
// c ends).
func (c *Cuts) Join(next *Cuts) error {
	if next.base != c.rows {
		return fmt.Errorf("the cuts after row %d do not follow those through row %d", next.base, c.rows)
	}
	if next.rows == next.base {
		return nil
	}
	for c.unit < next.unit {
		c.coarsen()
	}
	for next.unit < c.unit {
		next.coarsen()
	}
	c.kept = append(c.kept, next.kept...)
	c.rows, c.last = next.rows, next.last
	c.measure()
	for len(c.kept) > c.maxCuts || c.bytes > c.maxBytes {
		c.coarsen()
	}
	return nil
}

// Unit is R, the rows a cut.
func (c *Cuts) Unit() int64 { return c.unit }

// File is the cut file: nil for a dataset of no rows, an error when its
// last key cannot be a line.
func (c *Cuts) File() ([]byte, error) {
	if c.rows == c.base {
		return nil, nil
	}
	if !lineKey(c.last) {
		return nil, fmt.Errorf("the dataset's last key cannot close a line of the cut file (%.64q)", c.last)
	}
	for {
		tail := len(c.kept) == 0 || c.kept[len(c.kept)-1].rows != c.rows
		lines, size := len(c.kept), c.bytes
		if tail {
			lines, size = lines+1, size+lineBytes(c.last, c.rows-c.lastRows())
		}
		if lines <= c.maxCuts && size <= c.maxBytes {
			break
		}
		if len(c.kept) == 0 {
			return nil, errors.New("the dataset's last key alone is over the cut file's bounds")
		}
		c.coarsen()
	}
	var b bytes.Buffer
	b.Grow(int(c.bytes) + len(c.last) + 22)
	from := c.base
	line := func(key string, rows int64) {
		if !lineKey(key) {
			return
		}
		b.WriteString(key)
		b.WriteByte('\t')
		b.WriteString(strconv.FormatInt(rows-from, 10))
		b.WriteByte('\n')
		from = rows
	}
	for _, k := range c.kept {
		line(k.key, k.rows)
	}
	if from != c.rows {
		line(c.last, c.rows)
	}
	return b.Bytes(), nil
}

// coarsen doubles R: a cut stays where the rows reach a multiple of 2R
// since the cut before it.
func (c *Cuts) coarsen() {
	c.unit *= 2
	prev, n := c.base, 0
	for _, k := range c.kept {
		if k.rows/c.unit > prev/c.unit {
			c.kept[n] = k
			n++
		}
		prev = k.rows
	}
	clear(c.kept[n:])
	c.kept = c.kept[:n]
	c.measure()
}

// measure recounts the kept lines' bytes.
func (c *Cuts) measure() {
	c.bytes = 0
	prev := c.base
	for _, k := range c.kept {
		c.bytes += lineBytes(k.key, k.rows-prev)
		prev = k.rows
	}
}

// lastRows is the rows through the last cut kept.
func (c *Cuts) lastRows() int64 {
	if n := len(c.kept); n > 0 {
		return c.kept[n-1].rows
	}
	return c.base
}

// lineBytes is the bytes of a line of key with rows: an unwritten key's
// are counted too, at least the digits the next line gains.
func lineBytes(key string, rows int64) int64 {
	return int64(len(key)) + 2 + int64(len(strconv.FormatInt(rows, 10)))
}

// lineKey reports whether key can be a line: no line feed, and the line
// within MaxCutLine at any count.
func lineKey(key string) bool {
	return !strings.Contains(key, "\n") && len(key)+22 <= MaxCutLine
}
