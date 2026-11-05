// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package walk

import (
	"errors"
	"slices"
	"strings"
	"unicode/utf8"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

const kindInvalid record.Kind = 255 // slab slot whose stat failed; compacted away

// readFast is the ordered-mode reader for the getdents engine. Per directory
// it makes: one key arena ([]byte, no pointers), one row slab sized from the
// dirent count, one children slice, plus one path string and one node per
// subdirectory. Names are parsed into a reused per-worker arena (no string
// per name). Entries are sorted by key *before* stat, so each stat result is
// written straight into its final slot: no row is copied or moved afterwards.
// (x/sys still makes one short C-string allocation per fstatat; a raw-syscall
// variant avoiding it was measured and gave no wall-time gain, so it was
// dropped.)
// ok=false means "use the generic path" (DT_UNKNOWN entries present).
func readFast(w *worker, job dirJob, statxMask uint32) (rows []record.Row, kids []kid, ok bool) {
	flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC
	if job.prefix != "" {
		flags |= unix.O_NOFOLLOW
	}
	fd, err := unix.Open(job.path, flags, 0)
	if err != nil {
		w.stats.OpenErrors++
		return nil, nil, true
	}
	defer unix.Close(fd)
	if w.direntBuf == nil {
		w.direntBuf = make([]byte, 64<<10)
	}
	names := w.nameArena[:0]
	ents := w.fdents[:0]
	ndirs, nameBytes := 0, 0
	unknown := false
parse:
	for {
		n, err := unix.Getdents(fd, w.direntBuf)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			w.stats.ReadDirErrors++
			break
		}
		if n <= 0 {
			break
		}
		buf := w.direntBuf[:n]
		for len(buf) >= 19 {
			reclen := int(*(*uint16)(unsafe.Pointer(&buf[16])))
			if reclen <= 19 || reclen > len(buf) {
				w.stats.ReadDirErrors++
				break parse
			}
			rec := buf[:reclen]
			buf = buf[reclen:]
			ino := *(*uint64)(unsafe.Pointer(&rec[0]))
			name := rec[19:]
			if i := indexZero(name); i >= 0 {
				name = name[:i]
			}
			if ino == 0 || len(name) == 1 && name[0] == '.' || len(name) == 2 && name[0] == '.' && name[1] == '.' {
				continue
			}
			if !utf8.Valid(name) {
				w.stats.InvalidUTF8++
				continue
			}
			typ := rec[18]
			if typ == unix.DT_UNKNOWN {
				unknown = true
			}
			if typ == unix.DT_DIR {
				ndirs++
			}
			off := len(names)
			names = append(names, name...)
			ents = append(ents, fdent{nameOff: uint32(off), nameLen: uint32(len(name)), ino: ino, typ: typ})
			nameBytes += len(name)
		}
	}
	w.nameArena, w.fdents = names, ents
	if unknown {
		return nil, nil, false
	}
	// Keys: prefix+name (+"/" for directories) in one arena, sliced as strings.
	arena := make([]byte, 0, len(ents)*len(job.prefix)+nameBytes+ndirs)
	for i := range ents {
		e := &ents[i]
		start := len(arena)
		arena = append(arena, job.prefix...)
		arena = append(arena, names[e.nameOff:e.nameOff+e.nameLen]...)
		if e.typ == unix.DT_DIR {
			arena = append(arena, '/')
		}
		e.keyOff, e.keyLen = uint32(start), uint32(len(arena)-start)
	}
	keyOf := func(e *fdent) string {
		if e.keyLen == 0 {
			return ""
		}
		return unsafe.String(&arena[e.keyOff], int(e.keyLen))
	}
	slices.SortFunc(ents, func(a, b fdent) int { return strings.Compare(keyOf(&a), keyOf(&b)) })
	rows = make([]record.Row, len(ents))
	order := ents
	if w.opt.InoSort && w.opt.Fields != FieldsNames {
		for i := range ents {
			ents[i].pos = uint32(i)
		}
		order = slices.Clone(ents) // stat in d_ino order, write each result to its sorted slot
		slices.SortFunc(order, func(a, b fdent) int {
			switch {
			case a.ino < b.ino:
				return -1
			case a.ino > b.ino:
				return 1
			}
			return 0
		})
	} else {
		for i := range ents {
			ents[i].pos = uint32(i)
		}
	}
	resort := false
	var st unix.Stat_t
	var stx unix.Statx_t
	for i := range order {
		e := &order[i]
		r := &rows[e.pos]
		r.Key = keyOf(e)
		nameStr := unsafe.String(&names[e.nameOff], int(e.nameLen)) // zero-copy; names is not mutated in this phase
		wantDir := e.typ == unix.DT_DIR
		switch {
		case w.opt.Fields == FieldsNames:
			r.Kind = kindFromDT(e.typ)
		case statxMask != 0:
			err = unix.Statx(fd, nameStr, unix.AT_SYMLINK_NOFOLLOW|unix.AT_NO_AUTOMOUNT, int(statxMask), &stx)
			if err == nil {
				fillStatx(r, &stx)
			}
		default:
			err = unix.Fstatat(fd, nameStr, &st, unix.AT_SYMLINK_NOFOLLOW)
			if err == nil {
				fillStat(r, &st)
			}
		}
		if w.opt.Fields != FieldsNames && err != nil {
			if errors.Is(err, unix.ENOENT) {
				w.stats.Vanished++
			} else {
				w.stats.StatErrors++
			}
			r.Kind = kindInvalid
			continue
		}
		if (r.Kind == record.KindDir) != wantDir { // type changed since getdents: fix key, resort
			if wantDir {
				r.Key = r.Key[:len(r.Key)-1]
			} else {
				r.Key += "/"
			}
			resort = true
		}
		if r.Kind == record.KindSymlink && w.opt.LinkTarget && w.opt.Fields != FieldsNames {
			if t, ok := readlinkat(w, fd, nameStr); ok {
				r.LinkTarget = t
				r.Present |= record.HasLink
			}
		}
	}
	// Compact failed and skipped slots (order-preserving), count, find children.
	out := rows[:0]
	for i := range rows {
		r := &rows[i]
		if r.Kind == kindInvalid {
			continue
		}
		if r.Kind == record.KindDir {
			if w.opt.SkipDirKeys[r.Key] {
				w.stats.SkippedDirs++
				continue
			}
			w.stats.Dirs++
		} else {
			w.stats.Files++
			w.stats.Bytes += r.Size
		}
		out = append(out, *r) // in place: index <= i
	}
	rows = out
	if resort {
		slices.SortFunc(rows, func(a, b record.Row) int { return strings.Compare(a.Key, b.Key) })
	}
	for i := range rows {
		if rows[i].Kind == record.KindDir {
			k := rows[i].Key
			name := k[len(job.prefix) : len(k)-1]
			kids = append(kids, kid{pos: int32(i), node: &dirNode{job: dirJob{path: job.path + "/" + name, prefix: k}}})
		}
	}
	return rows, kids, true
}

func indexZero(b []byte) int {
	for i, c := range b {
		if c == 0 {
			return i
		}
	}
	return -1
}

func fastReaderAvailable(opt *Options) bool { return opt.Engine == EngineGetdents }

func statxMaskFor(opt *Options) uint32 {
	if opt.Fields != FieldsStatx {
		return 0
	}
	if opt.StatxMask == "basic" {
		return unix.STATX_BASIC_STATS
	}
	return unix.STATX_ALL
}
