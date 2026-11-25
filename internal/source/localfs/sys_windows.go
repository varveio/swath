// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package localfs

import (
	"io/fs"
	"os"
	"syscall"
)

// statLinks: no tier reads a link count, device or inode here.
const statLinks = false

// statBlocks: no tier reads allocated blocks here.
const statBlocks = false

// fillSys: CreationTime is a birth time (Windows has no POSIX ctime through
// FileInfo) and LastAccessTime is the access time. uid, gid, inode, dev, nlink and
// blocks stay null.
func fillSys(r *Entry, fi fs.FileInfo) {
	d, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return
	}
	r.Btime = d.CreationTime.Nanoseconds() / 1000
	r.Atime = d.LastAccessTime.Nanoseconds() / 1000
	r.Present |= HasBtime | HasAtime
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
	d, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok || d.FileAttributes&surrogateDirectory != surrogateDirectory {
		return "", false
	}
	t, err := os.Readlink(path)
	return t, err == nil
}
