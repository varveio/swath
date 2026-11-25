// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package localfs

import (
	"io/fs"
	"os"
	"syscall"
)

// statLinks: no tier reads a link count or device here; the file ID is
// read where the filesystem has one, but nothing tells a hard-linked file
// from another (#364).
const statLinks = false

// statBlocks: a stat tier reads each entry's space on disk, from the
// directory enumeration (readDir).
const statBlocks = true

// fillSys: CreationTime is a birth time (Windows has no POSIX ctime through
// FileInfo) and LastAccessTime is the access time. From an entry of the
// directory enumeration (readDir), allocated_bytes is its AllocationSize
// and inode its file ID where the filesystem gives one; Lstat's (a file
// root, fillRoot) carries neither. uid, gid, dev and nlink stay null.
//
// NTFS keeps a copy of each file's length and allocation in every directory
// entry that names it, and refreshes only the entry the file was opened
// through, so the enumeration can lag: under a hard link's other names, and
// for a file being written until it is flushed or closed. A compressed or
// sparse file's figure is current once its data is written back; before
// that it is the uncompressed or unallocated one, as every other way to ask
// says (docs/internals/measurements.md).
func fillSys(r *Entry, fi fs.FileInfo) {
	d, w := winData(fi)
	if d == nil {
		return
	}
	r.Btime = d.CreationTime.Nanoseconds() / 1000
	r.Atime = d.LastAccessTime.Nanoseconds() / 1000
	r.Present |= HasBtime | HasAtime
	if w == nil || true { // DEMO ONLY: the values not filled, to show the tests fail
		return
	}
	r.Blocks, r.Present = w.AllocationSize, r.Present|HasBlocks
	if w.FileID != 0 {
		r.Inode, r.Present = w.FileID, r.Present|HasInode
	}
}

// winData is fi's Win32FileAttributeData, and the enumeration's own where
// fi is readDir's.
func winData(fi fs.FileInfo) (*syscall.Win32FileAttributeData, *winInfo) {
	switch s := fi.Sys().(type) {
	case *winInfo:
		return &s.Win32FileAttributeData, s
	case *syscall.Win32FileAttributeData:
		return s, nil
	}
	return nil, nil
}

// reparseLinks: a junction or a volume mount point lists as a symbolic link
// (mountPoint), and the walk never enters one.
const reparseLinks = true

// surrogateDirectory is a directory that is a reparse point.
const surrogateDirectory = syscall.FILE_ATTRIBUTE_REPARSE_POINT | syscall.FILE_ATTRIBUTE_DIRECTORY

// mountPoint reads the entry at path, of kind unknown and described by fi
// (the directory enumeration's own: no call), as a junction or a volume
// mount point: ok with its target, or not one. Go reports a directory
// reparse point whose tag is a name surrogate without the directory bit
// and, but for a symbolic link's tag, as ModeIrregular: kind unknown. Its
// attributes say only that much, so the tag decides: os.Readlink reads
// IO_REPARSE_TAG_MOUNT_POINT's target, as a symbolic link's, and fails for
// any other tag (IO_REPARSE_TAG_WCI_LINK, say), which stays unknown, as
// does an entry whose read fails. It opens the entry without following it
// and asks for no access, so a junction whose list permission is denied
// (C:\Documents and Settings) is read too. One call per such entry, which
// is rare; the target tells a junction (a path) from a mount point
// (\\?\Volume{GUID}\). A directory reparse point of another tag (a cloud
// placeholder, a DFS link) keeps the directory bit and is walked.
func mountPoint(fi fs.FileInfo, path string) (string, bool) {
	d, w := winData(fi)
	if d == nil || d.FileAttributes&surrogateDirectory != surrogateDirectory || w != nil && w.ReparseTag != ioReparseTagMountPoint {
		return "", false // readDir's entries carry the tag: no read for another one
	}
	t, err := os.Readlink(path)
	return t, err == nil
}
