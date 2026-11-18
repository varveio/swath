// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package localfs

import (
	"io/fs"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/internal/schema"
)

// statAt fills r from one stat of name relative to dirfd, never following a
// symlink or triggering an automount.
func statAt(dirfd int, name string, f Fields, r *Entry) error {
	if f == Statx {
		var s unix.Statx_t
		if err := unix.Statx(dirfd, name, unix.AT_SYMLINK_NOFOLLOW|unix.AT_NO_AUTOMOUNT, unix.STATX_ALL, &s); err != nil {
			return err
		}
		fillStatx(r, &s)
		return nil
	}
	var s unix.Stat_t
	if err := unix.Fstatat(dirfd, name, &s, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	fillStat(r, &s)
	return nil
}

// dtKinds maps d_type values (0..15) to kinds; unlisted values are unknown.
var dtKinds = [16]Kind{
	unix.DT_REG: KindFile, unix.DT_DIR: KindDir, unix.DT_LNK: KindSymlink,
	unix.DT_FIFO: KindFIFO, unix.DT_SOCK: KindSocket, unix.DT_CHR: KindCharDevice,
	unix.DT_BLK: KindBlockDevice,
}

func kindOfDT(t uint8) Kind { return dtKinds[t&15] }

func dtypeOf(k Kind) uint8 {
	for t, kk := range dtKinds {
		if kk == k && k != KindUnknown {
			return uint8(t)
		}
	}
	return unix.DT_UNKNOWN
}

// statxPresence pairs the statx fields an Entry reads with the presence
// they give. The kernel may leave a requested field out (a filesystem
// that does not keep it: blocks on some network filesystems); its value
// is then null, never the zero statx leaves.
var statxPresence = [...]struct {
	mask uint32
	has  Presence
}{
	{unix.STATX_SIZE, HasSize}, {unix.STATX_MTIME, HasMtime}, {unix.STATX_MODE, HasMode},
	{unix.STATX_UID, HasUID}, {unix.STATX_GID, HasGID}, {unix.STATX_NLINK, HasNlink},
	{unix.STATX_INO, HasInode}, {unix.STATX_BLOCKS, HasBlocks}, {unix.STATX_CTIME, HasCtime},
	{unix.STATX_ATIME, HasAtime}, {unix.STATX_BTIME, HasBtime},
}

// fillStatx fills r from s, each value present only where s.Mask says the
// kernel returned it; the device, and the file type the walk checks
// against the directory entry's, are always returned.
func fillStatx(r *Entry, s *unix.Statx_t) {
	r.Kind = kindOfMode(uint32(s.Mode))
	r.Size = int64(s.Size)
	r.Mode = uint32(s.Mode) & 0o7777
	r.UID, r.GID, r.Nlink = s.Uid, s.Gid, s.Nlink
	r.Inode, r.Dev, r.Blocks = int64(s.Ino), int64(unix.Mkdev(s.Dev_major, s.Dev_minor)), int64(s.Blocks)*512
	r.Mtime = schema.Micros(s.Mtime.Sec, int64(s.Mtime.Nsec))
	r.Ctime = schema.Micros(s.Ctime.Sec, int64(s.Ctime.Nsec))
	r.Atime = schema.Micros(s.Atime.Sec, int64(s.Atime.Nsec))
	r.Btime = schema.Micros(s.Btime.Sec, int64(s.Btime.Nsec))
	r.Present |= HasDev
	for _, f := range statxPresence {
		if s.Mask&f.mask != 0 {
			r.Present |= f.has
		}
	}
}

// statLinks: a stat tier reads each entry's link count, device and inode.
const statLinks = true

// statBlocks: a stat tier reads each entry's allocated blocks.
const statBlocks = true

// fillSys adds what os.FileInfo hides behind Sys() (portable engine).
func fillSys(r *Entry, fi fs.FileInfo) {
	s, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	r.UID, r.GID, r.Nlink = s.Uid, s.Gid, uint32(s.Nlink)
	r.Inode, r.Dev, r.Blocks = int64(s.Ino), int64(s.Dev), s.Blocks*512
	r.Mtime = schema.Micros(s.Mtim.Unix())
	r.Ctime = schema.Micros(s.Ctim.Unix())
	r.Atime = schema.Micros(s.Atim.Unix())
	r.Mode = s.Mode & 0o7777
	r.Present |= HasStat
}
