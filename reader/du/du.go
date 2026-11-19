// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package du

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/varveio/swath/reader/listing"
)

var (
	// ErrInodeLimit fails an Add that would remember more hard-linked files
	// than Options.MaxInodes.
	ErrInodeLimit = errors.New("du: more hard-linked files than the limit")
	// ErrOverflow fails an Add that would overflow a sum.
	ErrOverflow = errors.New("du: a sum overflows int64")
)

// defaultMaxInodes is Options.MaxInodes when it is 0.
const defaultMaxInodes = 1_000_000

// Options say which prefixes a Tally sums.
type Options struct {
	// Prefixes are "" or keys ending in "/", nested or not, none repeated.
	Prefixes  []string
	MaxInodes int // hard-linked files with a name in some prefix (0: 1,000,000)
}

// New returns an empty Tally. It copies opt and refuses a malformed or repeated prefix,
// or a negative MaxInodes, with an error that matches listing.ErrInvalidRequest.
func New(opt Options) (*Tally, error) {
	if opt.MaxInodes < 0 {
		return nil, fmt.Errorf("du: MaxInodes %d is negative: %w", opt.MaxInodes, listing.ErrInvalidRequest)
	}

	t := &Tally{
		prefixes:  slices.Clone(opt.Prefixes),
		index:     make(map[string]int32, len(opt.Prefixes)),
		maxInodes: opt.MaxInodes,
		sums:      make([]sums, len(opt.Prefixes)),
		seen:      map[fileID]member{},
		stage: staging{
			sums:    make([]sums, len(opt.Prefixes)),
			touched: make([]bool, len(opt.Prefixes)),
			grown:   map[fileID][]int32{},
		},
	}
	if t.maxInodes == 0 {
		t.maxInodes = defaultMaxInodes
	}
	for i, p := range t.prefixes {
		if (p != "" && !strings.HasSuffix(p, "/")) || strings.IndexByte(p, 0) >= 0 {
			return nil, fmt.Errorf("du: prefix %q is neither \"\" nor a key ending in \"/\": %w", p, listing.ErrInvalidRequest)
		}
		if _, ok := t.index[p]; ok {
			return nil, fmt.Errorf("du: prefix %q is repeated: %w", p, listing.ErrInvalidRequest)
		}
		t.index[p] = int32(i)
		t.longest = max(t.longest, len(p))
	}
	return t, nil
}

// Tally is the sums. It is not safe for concurrent use: a Walk calls emit on one goroutine.
type Tally struct {
	prefixes  []string
	index     map[string]int32 // a prefix's position in prefixes
	longest   int              // the longest prefix's length
	maxInodes int

	sums  []sums            // per prefix, what Add took
	seen  map[fileID]member // per hard-linked file, the prefixes that counted it
	err   error             // the error that failed an Add, and fails every later one
	stage staging           // the slice Add is taking
}

// fileID is a hard-linked file's identity: its device and inode.
type fileID struct{ dev, ino uint64 }

// member is the prefixes that have counted one hard-linked file, by their
// position in Options.Prefixes: the union of its names' prefixes.
type member struct {
	first int32
	more  []int32
}

func (m member) has(j int32) bool {
	return m.first == j || slices.Contains(m.more, j)
}

// sums is one prefix's figures.
type sums struct {
	files, symlinks, size, allocated int64
	sizeMissing, newestMissing       int64
	allocatedMissing, unidentified   int64
	newest                           time.Time
	hasNewest                        bool
}

// staging is what Add has taken of its slice so far, committed only when
// it takes the whole slice.
type staging struct {
	// sums is, for each prefix touched, its committed figures plus the
	// slice's; touched marks those prefixes and order lists them.
	sums    []sums
	touched []bool
	order   []int32
	// grown is, per hard-linked file, the prefixes the slice newly counts
	// it in, for files already remembered and new ones alike; added is how
	// many are new.
	grown map[fileID][]int32
	added int
	match []int32 // the current entry's prefixes
}

// Add counts entries in each prefix their keys begin with; a KindDir at key "" is
// the root's own entry. It keeps nothing of entries and takes all or none: on an
// invalid kind, a negative figure, ErrOverflow or ErrInodeLimit it takes none, and
// every later Add fails with the same error.
func (t *Tally) Add(entries []listing.Entry) error {
	if t.err != nil {
		return t.err
	}

	for i := range entries {
		if err := t.take(&entries[i]); err != nil {
			t.discard()
			t.err = err
			return err
		}
	}

	t.commit()
	return nil
}

// Result is every prefix's totals over the entries taken, in Options.Prefixes'
// order, as new values. It does not change the tally.
func (t *Tally) Result() []Totals {
	out := make([]Totals, len(t.prefixes))
	for j, p := range t.prefixes {
		s := &t.sums[j]
		out[j] = Totals{
			Prefix:           p,
			Files:            s.files,
			Symlinks:         s.symlinks,
			Size:             s.size,
			Allocated:        s.allocated,
			SizeMissing:      s.sizeMissing,
			NewestMissing:    s.newestMissing,
			AllocatedMissing: s.allocatedMissing,
			Unidentified:     s.unidentified,
		}
		if s.hasNewest {
			newest := s.newest
			out[j].Newest = &newest
		}
	}
	return out
}

// Totals is one prefix's sums. Allocated never exceeds du -sB1 of the prefix on an
// acyclic tree; Files, Size and Newest never exceed their values over every entry
// under the prefix. The missing counters are diagnostics, not bounds.
type Totals struct {
	Prefix   string
	Files    int64 // counted KindFile and KindObject entries, a hard-linked file once
	Symlinks int64 // KindSymlink names, counted or not
	// Size and Allocated sum the counted entries' lengths and space on disk,
	// Allocated with directories' own blocks; Newest is the latest LastModified
	// of the counted files and objects.
	Size, Allocated int64
	Newest          *time.Time
	// Per name: counted files and objects without a size or a time; directories
	// and local entries without AllocatedBytes, and local entries without identity
	// (Links unknown or 0, or above 1 without Device or Inode), the last two counted in
	// no other figure.
	SizeMissing, NewestMissing     int64
	AllocatedMissing, Unidentified int64
}

// part is what one entry adds to each prefix that holds its key.
type part struct {
	symlink bool // a symbolic link's name, counted or not
	// allocatedMissing and unidentified exclude a local entry from every
	// figure but Symlinks; counted is an entry that adds the figures below.
	allocatedMissing, unidentified, counted bool

	file                       bool // counts in Files
	size, allocated            int64
	sizeMissing, newestMissing bool
	newest                     time.Time
	hasNewest                  bool

	// linked is a counted file with several links, id its identity: it
	// counts once within each prefix.
	linked bool
	id     fileID
}

// take stages one entry.
func (t *Tally) take(e *listing.Entry) error {
	switch e.Kind {
	case listing.KindPrefix, listing.KindUnknown:
		return nil
	case listing.KindObject, listing.KindFile, listing.KindDir, listing.KindSymlink, listing.KindOther:
	default:
		return fmt.Errorf("du: entry %q has no kind of a listing (%v): %w", e.Key, e.Kind, listing.ErrInvalidRequest)
	}
	if negative(e.Size) || negative(e.AllocatedBytes) {
		return fmt.Errorf("du: entry %q has a negative size or space on disk: %w", e.Key, listing.ErrInvalidRequest)
	}

	match := t.match(e.Key)
	if len(match) == 0 {
		return nil
	}

	p := partOf(e)
	for _, j := range match {
		s := t.staged(j)
		if p.symlink {
			s.symlinks++
		}
		switch {
		case p.allocatedMissing:
			s.allocatedMissing++
			continue
		case p.unidentified:
			s.unidentified++
			continue
		}
		if p.linked {
			first, err := t.claim(p.id, j)
			if err != nil {
				return err
			}
			if !first {
				continue
			}
		}
		if err := s.add(&p); err != nil {
			return err
		}
	}
	return nil
}

func negative(v *int64) bool { return v != nil && *v < 0 }

// partOf is what e, of a kind Add counts, adds to each of its prefixes.
func partOf(e *listing.Entry) part {
	var p part
	switch e.Kind {
	case listing.KindObject:
		p.counted, p.file = true, true
		p.times(e)
		return p
	case listing.KindDir:
		if e.AllocatedBytes == nil {
			p.allocatedMissing = true
			return p
		}
		p.counted, p.allocated = true, *e.AllocatedBytes
		return p
	}

	// A local entry other than a directory.
	p.symlink = e.Kind == listing.KindSymlink
	identified := e.Links != nil && (*e.Links == 1 || (*e.Links > 1 && e.Device != nil && e.Inode != nil))
	switch {
	case e.AllocatedBytes == nil:
		p.allocatedMissing = true
		return p
	case !identified:
		p.unidentified = true
		return p
	}

	p.counted, p.allocated = true, *e.AllocatedBytes
	if e.Kind == listing.KindFile {
		p.file = true
		p.times(e)
	} else if e.Size != nil {
		p.size = *e.Size
	}
	if *e.Links > 1 {
		p.linked, p.id = true, fileID{dev: *e.Device, ino: *e.Inode}
	}
	return p
}

// times takes a file's or an object's size and time, each missing or not.
func (p *part) times(e *listing.Entry) {
	if e.Size != nil {
		p.size = *e.Size
	} else {
		p.sizeMissing = true
	}
	if e.LastModified != nil {
		p.newest, p.hasNewest = *e.LastModified, true
	} else {
		p.newestMissing = true
	}
}

// add adds a counted entry's figures, or fails with ErrOverflow and adds
// none.
func (s *sums) add(p *part) error {
	if p.size > math.MaxInt64-s.size || p.allocated > math.MaxInt64-s.allocated {
		return ErrOverflow
	}

	s.size += p.size
	s.allocated += p.allocated
	if p.file {
		s.files++
	}
	if p.sizeMissing {
		s.sizeMissing++
	}
	if p.newestMissing {
		s.newestMissing++
	}
	if p.hasNewest && (!s.hasNewest || p.newest.After(s.newest)) {
		s.newest, s.hasNewest = p.newest, true
	}
	return nil
}

// match is the positions of the requested prefixes key begins with: one
// map lookup per "/" in key, up to the longest prefix. The slice is
// reused by the next call.
func (t *Tally) match(key []byte) []int32 {
	m := t.stage.match[:0]
	if j, ok := t.index[""]; ok {
		m = append(m, j)
	}
	head := key[:min(len(key), t.longest)]
	for n := 0; ; {
		i := bytes.IndexByte(head[n:], '/')
		if i < 0 {
			break
		}
		n += i + 1
		if j, ok := t.index[string(head[:n])]; ok {
			m = append(m, j)
		}
	}
	t.stage.match = m
	return m
}

// staged is prefix j's figures with the slice's so far.
func (t *Tally) staged(j int32) *sums {
	st := &t.stage
	if !st.touched[j] {
		st.touched[j] = true
		st.sums[j] = t.sums[j]
		st.order = append(st.order, j)
	}
	return &st.sums[j]
}

// claim reports whether prefix j has yet to count the hard-linked file id,
// in what Add took before or in this slice, and stages the membership when
// it has; it fails with ErrInodeLimit rather than remember one file too
// many.
func (t *Tally) claim(id fileID, j int32) (bool, error) {
	m, remembered := t.seen[id]
	if remembered && m.has(j) {
		return false, nil
	}

	st := &t.stage
	grown, staged := st.grown[id]
	if slices.Contains(grown, j) {
		return false, nil
	}
	if !remembered && !staged {
		if len(t.seen)+st.added >= t.maxInodes {
			return false, ErrInodeLimit
		}
		st.added++
	}

	st.grown[id] = append(grown, j)
	return true, nil
}

// commit makes the staged slice the tally's.
func (t *Tally) commit() {
	st := &t.stage
	for _, j := range st.order {
		t.sums[j] = st.sums[j]
	}

	for id, grown := range st.grown {
		m, ok := t.seen[id]
		if !ok {
			m = member{first: grown[0]}
			if len(grown) > 1 {
				m.more = slices.Clone(grown[1:])
			}
			t.seen[id] = m
			continue
		}
		more := make([]int32, 0, len(m.more)+len(grown))
		m.more = append(append(more, m.more...), grown...)
		t.seen[id] = m
	}

	t.discard()
}

// discard drops the staged slice.
func (t *Tally) discard() {
	st := &t.stage
	for _, j := range st.order {
		st.touched[j] = false
	}
	st.order = st.order[:0]
	clear(st.grown)
	st.added = 0
}
