// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package localfs

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/go/internal/source"
)

const nativeEngine = true

// Large directories are statted in chunks by several workers.
const (
	chunkThreshold = 4096 // entries above which a directory is chunked
	chunkEntries   = 2048 // entries per chunk
	direntBuffer   = 64 << 10
	// direntHintSlack: a read that returns more than the buffer less this
	// (the largest record, 280 bytes, with room) filled it, so the
	// directory likely goes on.
	direntHintSlack = 1 << 10
	cancelEvery     = 1024 // entries between context checks

	// A directory of at least parallelSortMin entries is sorted in
	// parallel runs of at least sortRunMin entries, then merged.
	parallelSortMin = 1 << 15
	sortRunMin      = 1 << 14

	// After presizeAfter entries, a directory's buffers are grown once to
	// the entry count its size suggests, rather than doubling repeatedly.
	presizeAfter = 1024
	presizeMax   = 1 << 24 // entries: an estimate is never trusted beyond this
	// readdirplusHint is how many entries of a full batch are statted before
	// the next read: more than the Linux NFS client's threshold of 8
	// (NFS_READDIR_CACHE_USAGE_THRESHOLD in fs/nfs/dir.c).
	readdirplusHint = 9
)

// entry is one directory entry: its name in the scratch name buffer and its
// key in the directory's key arena, as offsets. It holds no pointer, so a
// huge directory's entries are grown, sorted and handed to chunks without
// GC write barriers or scanning.
type entry struct {
	nameOff, nameLen uint32
	keyOff, keyLen   uint32
	typ              uint8
}

// errTooLarge: a directory whose names or keys exceed 4 GiB does not fit an
// entry's 32-bit offsets; it is skipped and counted as a read error.
var errTooLarge = errors.New("directory too large: its names or keys exceed 4 GiB")

// fitsOffsets reports whether sc's names, and the keys buildKeys would make
// under a prefix of plen bytes, fit an entry's 32-bit offsets.
func fitsOffsets(plen int, sc *scratch) bool {
	names := uint64(len(sc.names))
	return names <= math.MaxUint32 && names+uint64(len(sc.ents))*uint64(plen+1) <= math.MaxUint32
}

// scratch is per-goroutine reusable memory for reading a directory. keys is
// not reused: the rows of the directory are views of it.
type scratch struct {
	buf   []byte
	names []byte
	ents  []entry
	keys  []byte
}

var scratchPool = sync.Pool{New: func() any { return &scratch{buf: make([]byte, direntBuffer)} }}

// chunk is a slice of a large directory's sorted entries sharing one fd.
type chunk struct {
	dir   *sharedFD
	names []byte
	keys  []byte
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

// disposeChunk releases a chunk range's share of its directory descriptor:
// after its Expand, or for a range that will never be expanded.
func disposeChunk(r Range) {
	if c := r.chunk; c != nil {
		c.dir.release()
	}
}

// expandNative lists a range with raw getdents64 and fd-relative stat.
// Per directory it allocates one key arena, one row slab sized from the
// entry count and one range per subdirectory; entries are sorted by key
// before stat, so each stat lands in its final slot.
func (p *Provider) expandNative(ctx context.Context, r Range, e source.Emitter[Range, Entry], st *Stats) error {
	if c := r.chunk; c != nil { // Expand releases its descriptor share
		rows, err := p.statInto(ctx, c.dir.fd, r.path, c.names, c.keys, c.ents, st)
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
	defer func() { sc.keys = nil; scratchPool.Put(sc) }() // the rows own the key arena
	if p.unsorted {
		defer unix.Close(fd)
		return p.streamNames(ctx, fd, r, sc, e, st)
	}
	if err := readNames(ctx, fd, r.path, sc, p.opt.Fields != Names, st); err != nil {
		unix.Close(fd)
		return err
	}
	if !fitsOffsets(len(r.prefix), sc) {
		unix.Close(fd)
		st.note(&st.ReadErrors, r.path, errTooLarge)
		return nil
	}
	p.resolveUnknown(fd, sc)
	buildKeys(r.prefix, sc)
	if err := sortChecked(ctx, len(r.prefix), sc, st); err != nil {
		unix.Close(fd)
		return err
	}

	if len(sc.ents) > chunkThreshold && p.opt.Fields != Names {
		return p.emitChunks(fd, r, sc, e, st)
	}
	defer unix.Close(fd)
	rows, err := p.statInto(ctx, fd, r.path, sc.names, sc.keys, sc.ents, st)
	if err != nil {
		return err
	}
	return p.emit(r, rows, e, st)
}

// streamNames lists a directory for a count, whose rows are neither sorted
// nor kept (opened.Run): each getdents batch is typed, keyed, statted and
// emitted before the next is read, as find does, so the directory's memory
// is one batch, not its whole listing. Holding a directory of 1M files
// (about 140 MiB) was what made a count thrash the page cache on a 512 MiB
// machine (issue #388). The batch's stats come before the next read, so an
// NFS client keeps fetching attributes with the names (readNames) without a
// hint.
func (p *Provider) streamNames(ctx context.Context, fd int, r Range, sc *scratch, e source.Emitter[Range, Entry], st *Stats) error {
	st.Unsorted++
	add := func(_ uint64, typ uint8, name []byte) {
		sc.ents = append(sc.ents, entry{nameOff: uint32(len(sc.names)), nameLen: uint32(len(name)), typ: typ})
		sc.names = append(sc.names, name...)
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := unix.Getdents(fd, sc.buf)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			st.note(&st.ReadErrors, r.path, err)
			return nil
		}
		if n <= 0 {
			return nil
		}
		sc.names, sc.ents = sc.names[:0], sc.ents[:0]
		if err := parseDirents(sc.buf[:n], add); err != nil {
			st.note(&st.ReadErrors, r.path, err)
			return nil
		}
		st.StreamedBatches++
		p.resolveUnknown(fd, sc)
		buildKeys(r.prefix, sc) // a new arena: the emitted rows are views of it
		rows, err := p.statInto(ctx, fd, r.path, sc.names, sc.keys, sc.ents, st)
		if err != nil {
			return err
		}
		if err := p.emit(r, rows, e, st); err != nil {
			return err
		}
	}
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
//
// With stat, a batch that fills most of the buffer (the directory goes on)
// has a few of its entries statted before the next read. The Linux NFS
// client asks the server for attributes with the names (READDIRPLUS) on a
// directory's first read, and on a later read only if more than 8 of its
// entries were looked up since the one before (nfs_use_readdirplus); a
// directory read whole before any stat otherwise costs one LOOKUP per entry
// beyond the first page. fdu (github.com/jlevy/fdu), which stats each batch
// before reading the next, keeps READDIRPLUS on for the same reason. On a
// local filesystem the hint is a few extra cached stats per 2,048 entries.
// The client's first reads of a directory can be short (19, then 5 entries
// on Linux 7.0) and are not hinted; hinting after them too was measured
// worse there (3,277 LOOKUPs on a 200,000-entry directory against none).
func readNames(ctx context.Context, fd int, path string, sc *scratch, stat bool, st *Stats) error {
	sc.names, sc.ents = sc.names[:0], sc.ents[:0]
	add := func(_ uint64, typ uint8, name []byte) {
		sc.ents = append(sc.ents, entry{nameOff: uint32(len(sc.names)), nameLen: uint32(len(name)), typ: typ})
		sc.names = append(sc.names, name...)
	}
	presized := false
	for batches := 0; ; batches++ {
		if batches%64 == 63 && ctx.Err() != nil { // a huge directory: stay cancellable
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
		first := len(sc.ents)
		if err := parseDirents(sc.buf[:n], add); err != nil {
			st.note(&st.ReadErrors, path, err)
			return nil
		}
		if stat && n > len(sc.buf)-direntHintSlack {
			hintReaddirplus(fd, sc, first)
			st.ReaddirplusHints++
		}
		if !presized && len(sc.ents) >= presizeAfter {
			presized = true
			if presize(fd, sc) {
				st.Presized++
			}
		}
	}
}

// presize grows sc's buffers once for a large directory, to the entry count
// its st_size suggests at the average name length seen so far: a linux
// dirent record is at least 8 bytes plus the name, 4-aligned, on ext4, and
// tmpfs and btrfs report sizes at or above that per entry, so the estimate
// is rarely short (appending still grows past it if it is). It reports
// whether it grew anything.
func presize(fd int, sc *scratch) bool {
	var s unix.Stat_t
	if unix.Fstat(fd, &s) != nil || s.Size <= 0 {
		return false
	}
	avg := len(sc.names) / len(sc.ents)
	est := min(int(s.Size/int64(8+(avg+3)&^3)), presizeMax)
	if est <= cap(sc.ents) {
		return false
	}
	sc.ents = slices.Grow(sc.ents, est-len(sc.ents))
	if more := est*avg - len(sc.names); more > 0 { // avg is rounded down: the names may already exceed it
		sc.names = slices.Grow(sc.names, more)
	}
	return true
}

// hintReaddirplus stats up to readdirplusHint entries from first, ignoring
// the results (readNames).
func hintReaddirplus(fd int, sc *scratch, first int) {
	var s unix.Stat_t
	for i := first; i < min(first+readdirplusHint, len(sc.ents)); i++ {
		unix.Fstatat(fd, nameOf(sc.names, &sc.ents[i]), &s, unix.AT_SYMLINK_NOFOLLOW)
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
// one new arena, sc.keys, recording each entry's offset and length.
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
		e.keyOff, e.keyLen = uint32(start), uint32(len(arena)-start)
	}
	sc.keys = arena
}

// sortChecked sorts a directory's entries, checking for a stop around the
// sort: a huge directory's sort is not interruptible.
func sortChecked(ctx context.Context, plen int, sc *scratch, st *Stats) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if sortEntries(plen, sc) {
		st.ParallelSorts++
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// sortEntries sorts a directory's entries by key, in parallel for a large
// directory, and reports whether it did so in parallel.
func sortEntries(plen int, sc *scratch) bool {
	runs := 1
	if len(sc.ents) >= parallelSortMin { // GOMAXPROCS(0) takes the scheduler lock: not per small directory
		runs = max(1, min(runtime.GOMAXPROCS(0), len(sc.ents)/sortRunMin))
	}
	sortRuns(plen, sc, runs)
	return runs > 1
}

// sortRuns sorts sc.ents by key: it splits them into runs sorted
// concurrently, then merges pairs of runs, concurrently, until one is
// left. Every key starts with the directory's prefix (plen bytes), so
// comparing what follows it, the name and a directory's "/", gives the same
// byte order without re-reading the prefix in every comparison. Names in a
// directory are distinct, so no two keys are equal and the result is the
// one order whatever the runs.
func sortRuns(plen int, sc *scratch, runs int) {
	keys := sc.keys
	cmp := func(a, b entry) int {
		return bytes.Compare(keys[int(a.keyOff)+plen:int(a.keyOff+a.keyLen)], keys[int(b.keyOff)+plen:int(b.keyOff+b.keyLen)])
	}
	src := sc.ents
	if runs <= 1 {
		slices.SortFunc(src, cmp)
		return
	}
	bounds := make([]int, runs+1)
	for i := range bounds {
		bounds[i] = i * len(src) / runs
	}
	var wg sync.WaitGroup
	for i := range runs {
		wg.Go(func() { slices.SortFunc(src[bounds[i]:bounds[i+1]], cmp) })
	}
	wg.Wait()
	dst := make([]entry, len(src))
	for len(bounds) > 2 {
		next := []int{0}
		for i := 0; i+1 < len(bounds); i += 2 {
			lo, hi := bounds[i], bounds[min(i+2, len(bounds)-1)]
			mid := bounds[i+1]
			next = append(next, hi)
			if i+2 >= len(bounds) { // an odd run out: carried over as it is
				copy(dst[lo:hi], src[lo:hi])
				continue
			}
			wg.Go(func() { mergeRuns(dst[lo:hi], src[lo:mid], src[mid:hi], cmp) })
		}
		wg.Wait()
		src, dst, bounds = dst, src, next
	}
	sc.ents = src
}

// mergeRuns merges the sorted runs a and b into dst (len(a)+len(b)).
func mergeRuns(dst, a, b []entry, cmp func(a, b entry) int) {
	i, j, k := 0, 0, 0
	for i < len(a) && j < len(b) {
		if cmp(a[i], b[j]) <= 0 {
			dst[k] = a[i]
			i++
		} else {
			dst[k] = b[j]
			j++
		}
		k++
	}
	k += copy(dst[k:], a[i:])
	copy(dst[k:], b[j:])
}

// keyOf views an entry's key in the arena without copying; the arena is
// never written once built, and rows keep it alive.
func keyOf(keys []byte, e *entry) string {
	return unsafe.String(unsafe.SliceData(keys[e.keyOff:]), int(e.keyLen))
}

// emitChunks shares fd among chunk ranges of the sorted entries. The chunks
// take the scratch's name and entry buffers rather than copies of them; the
// scratch goes back to the pool without them, so a huge directory's buffers
// are not kept for the next directory.
func (p *Provider) emitChunks(fd int, r Range, sc *scratch, e source.Emitter[Range, Entry], st *Stats) error {
	names, keys, ents := sc.names, sc.keys, sc.ents
	sc.names, sc.keys, sc.ents = nil, nil, nil
	n := (len(ents) + chunkEntries - 1) / chunkEntries
	d := &sharedFD{fd: fd}
	d.refs.Store(int32(n))
	st.Chunks += int64(n)
	for i := 0; i < len(ents); i += chunkEntries {
		c := &chunk{dir: d, names: names, keys: keys, ents: ents[i:min(i+chunkEntries, len(ents))]}
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
func (p *Provider) statInto(ctx context.Context, fd int, path string, names, keys []byte, ents []entry, st *Stats) ([]Row, error) {
	rows := make([]Row, len(ents))
	n := 0
	for i := range ents {
		if i%cancelEvery == cancelEvery-1 {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		e := &ents[i]
		r := &rows[n]
		r.Key = keyOf(keys, e)
		if p.opt.Fields == Names {
			r.Data.Kind = kindOfDT(e.typ)
			n++
			continue
		}
		name := nameOf(names, e)
		if err := statAt(fd, name, p.opt.Fields, &r.Data); err != nil {
			full := path + string(os.PathSeparator) + name
			if errors.Is(err, unix.ENOENT) {
				st.note(&st.Vanished, full, err)
			} else {
				st.note(&st.StatErrors, full, err)
			}
			*r = Row{}
			continue
		}
		if (r.Data.Kind == KindDir) != (e.typ == unix.DT_DIR) {
			st.note(&st.Changed, path+string(os.PathSeparator)+name, errors.New("type changed during listing"))
			*r = Row{}
			continue
		}
		if r.Data.Kind == KindSymlink && p.opt.LinkTarget {
			if t, err := readlinkAt(fd, name); err == nil {
				r.Data.LinkTarget, r.Data.Present = t, r.Data.Present|HasLink
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
