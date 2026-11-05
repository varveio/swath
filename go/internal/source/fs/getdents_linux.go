// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package fs

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

const nativeEngine = true

// Large directories are statted in chunks by several workers.
const (
	chunkThreshold = 4096 // entries above which a directory is chunked
	chunkEntries   = 2048 // entries per chunk
	direntBuffer   = 64 << 10
	cancelEvery    = 1024 // entries between context checks
)

// entry is one directory entry: its name in the scratch name buffer and its
// key in the directory's key arena.
type entry struct {
	nameOff, nameLen uint32
	key              string // slice of the key arena
	ino              uint64
	typ              uint8
}

// scratch is per-goroutine reusable memory for reading a directory.
type scratch struct {
	buf   []byte
	names []byte
	ents  []entry
}

var scratchPool = sync.Pool{New: func() any { return &scratch{buf: make([]byte, direntBuffer)} }}

// chunk is a slice of a large directory's sorted entries sharing one fd.
type chunk struct {
	dir   *sharedFD
	names []byte
	ents  []entry
}

type sharedFD struct {
	fd   int
	refs atomic.Int32
}

func (d *sharedFD) release() {
	if d.refs.Add(-1) == 0 {
		unix.Close(d.fd)
	}
}

// disposeChunk releases a chunk range that will never be expanded.
func disposeChunk(r Range) {
	if c := r.chunk; c != nil {
		c.dir.release()
	}
}

// expandNative lists a range with raw getdents64 and fd-relative stat.
// Per directory it allocates one key arena, one row slab sized from the
// entry count and one range per subdirectory; entries are sorted by key
// before stat, so each stat lands in its final slot.
func (p *Provider) expandNative(ctx context.Context, r Range, e source.Emitter[Range], st *Stats) error {
	if c := r.chunk; c != nil {
		defer c.dir.release()
		rows, err := p.statInto(ctx, c.dir.fd, r.path, c.names, c.ents, st)
		if err != nil {
			return err
		}
		return p.emit(r, rows, e, st)
	}
	fd, err := openDir(r.path, r.prefix != "")
	if err != nil {
		st.note(&st.OpenErrors, r.path, err)
		return nil
	}
	st.DirsRead++
	sc := scratchPool.Get().(*scratch)
	defer scratchPool.Put(sc)
	if err := readNames(ctx, fd, r.path, sc, st); err != nil {
		unix.Close(fd)
		return err
	}
	p.resolveUnknown(fd, sc)
	buildKeys(r.prefix, sc)
	if done(ctx) { // a sort of a huge directory is not interruptible: check around it
		unix.Close(fd)
		return ctx.Err()
	}
	slices.SortFunc(sc.ents, func(a, b entry) int { return strings.Compare(a.key, b.key) })
	if done(ctx) {
		unix.Close(fd)
		return ctx.Err()
	}

	if len(sc.ents) > chunkThreshold && p.opt.Fields != Names {
		return p.emitChunks(fd, r, sc, e, st)
	}
	defer unix.Close(fd)
	rows, err := p.statInto(ctx, fd, r.path, sc.names, sc.ents, st)
	if err != nil {
		return err
	}
	return p.emit(r, rows, e, st)
}

// openDir opens a directory for reading entries; below the root it never
// follows a symlink.
func openDir(path string, noFollow bool) (int, error) {
	flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC
	if noFollow {
		flags |= unix.O_NOFOLLOW
	}
	return unix.Open(path, flags, 0)
}

// readNames parses all entries of fd into sc (names and entries).
func readNames(ctx context.Context, fd int, path string, sc *scratch, st *Stats) error {
	sc.names, sc.ents = sc.names[:0], sc.ents[:0]
	add := func(ino uint64, typ uint8, name []byte) {
		if !utf8.Valid(name) {
			st.InvalidUTF8++
			return
		}
		sc.ents = append(sc.ents, entry{nameOff: uint32(len(sc.names)), nameLen: uint32(len(name)), ino: ino, typ: typ})
		sc.names = append(sc.names, name...)
	}
	for batches := 0; ; batches++ {
		if batches%64 == 63 && done(ctx) { // a huge directory: stay cancellable
			return ctx.Err()
		}
		n, err := unix.Getdents(fd, sc.buf)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			st.note(&st.ReadErrors, path, err)
			return nil
		}
		if n <= 0 {
			return nil
		}
		if err := parseDirents(sc.buf[:n], add); err != nil {
			st.note(&st.ReadErrors, path, err)
			return nil
		}
	}
}

// resolveUnknown types entries the filesystem reported as DT_UNKNOWN.
func (p *Provider) resolveUnknown(fd int, sc *scratch) {
	for i := range sc.ents {
		e := &sc.ents[i]
		if e.typ != unix.DT_UNKNOWN {
			continue
		}
		var s unix.Stat_t
		if unix.Fstatat(fd, nameOf(sc.names, e), &s, unix.AT_SYMLINK_NOFOLLOW) == nil {
			e.typ = dtypeOf(kindOfMode(s.Mode))
		}
	}
}

// buildKeys writes every key (prefix+name, plus "/" for directories) into
// one arena and slices it.
func buildKeys(prefix string, sc *scratch) {
	size := 0
	for i := range sc.ents {
		size += len(prefix) + int(sc.ents[i].nameLen) + 1
	}
	arena := make([]byte, 0, size)
	for i := range sc.ents {
		e := &sc.ents[i]
		start := len(arena)
		arena = append(arena, prefix...)
		arena = append(arena, sc.names[e.nameOff:e.nameOff+e.nameLen]...)
		if e.typ == unix.DT_DIR {
			arena = append(arena, '/')
		}
		e.key = unsafe.String(unsafe.SliceData(arena[start:]), len(arena)-start) // arena is never written again
	}
}

// emitChunks shares fd among chunk ranges of the sorted entries.
func (p *Provider) emitChunks(fd int, r Range, sc *scratch, e source.Emitter[Range], st *Stats) error {
	names := slices.Clone(sc.names)
	ents := slices.Clone(sc.ents)
	n := (len(ents) + chunkEntries - 1) / chunkEntries
	d := &sharedFD{fd: fd}
	d.refs.Store(int32(n))
	st.Chunks += int64(n)
	for i := 0; i < len(ents); i += chunkEntries {
		c := &chunk{dir: d, names: names, ents: ents[i:min(i+chunkEntries, len(ents))]}
		if err := e.Child(Range{path: r.path, prefix: r.prefix, chunk: c}); err != nil {
			// The emitter owns chunk i now (the order stage disposes it);
			// release the references of the chunks never emitted.
			for j := i + chunkEntries; j < len(ents); j += chunkEntries {
				d.release()
			}
			return err
		}
	}
	return nil
}

// statInto stats ents (sorted) relative to fd into one row slab, dropping
// entries that vanished, failed or changed type since they were listed.
func (p *Provider) statInto(ctx context.Context, fd int, path string, names []byte, ents []entry, st *Stats) ([]schema.Row, error) {
	rows := make([]schema.Row, len(ents))
	n := 0
	for i := range ents {
		if i%cancelEvery == cancelEvery-1 {
			if done(ctx) {
				return nil, ctx.Err()
			}
		}
		e := &ents[i]
		r := &rows[n]
		r.Key = e.key
		if p.opt.Fields == Names {
			r.Kind = kindOfDT(e.typ)
			n++
			continue
		}
		name := nameOf(names, e)
		if err := statAt(fd, name, p.opt.Fields, r); err != nil {
			full := path + string(os.PathSeparator) + name
			if errors.Is(err, unix.ENOENT) {
				st.note(&st.Vanished, full, err)
			} else {
				st.note(&st.StatErrors, full, err)
			}
			*r = schema.Row{}
			continue
		}
		if (r.Kind == schema.KindDir) != (e.typ == unix.DT_DIR) {
			st.note(&st.Changed, path+string(os.PathSeparator)+name, errors.New("type changed during listing"))
			*r = schema.Row{}
			continue
		}
		if r.Kind == schema.KindSymlink && p.opt.LinkTarget {
			if t, err := readlinkAt(fd, name); err == nil {
				r.LinkTarget, r.Present = t, r.Present|schema.HasLink
			} else {
				st.note(&st.ReadlinkErrors, path+string(os.PathSeparator)+name, err)
			}
		}
		n++
	}
	return rows[:n], nil
}

// nameOf views an entry's name without copying; names is not modified while
// the view is in use.
func nameOf(names []byte, e *entry) string {
	return unsafe.String(unsafe.SliceData(names[e.nameOff:]), int(e.nameLen))
}
