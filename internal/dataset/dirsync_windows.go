// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package dataset

import (
	"errors"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/varveio/swath/internal/faults"
)

// syncDir flushes dir, through faults.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return flushDir(d, faults.Sync)
}

// SyncDirIn flushes directory name, opened through root (recovery acts
// relative to the locked output, never by a pathname that may name another
// directory by then).
func SyncDirIn(root *os.Root, name string) error {
	d, err := root.Open(name)
	if err != nil {
		return err
	}
	return flushDir(d, faults.Sync)
}

// probeDirSync flushes dir once, outside faults (it is not a step of the
// run's durability): a filesystem that refuses the flush refuses it here,
// before the run writes anything.
func probeDirSync(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return flushDir(d, func(f faults.File) error { return f.Sync() })
}

// flushDir flushes the directory d is open on with sync (FlushFileBuffers),
// and closes d. The flush needs a handle with write access, which os.Open
// and os.Root never give a directory, so the directory is opened again from
// d's handle (an NtCreateFile relative to it with an empty name, as
// ReOpenFile does) with FILE_APPEND_DATA, the least access the flush
// accepts: on a directory it is FILE_ADD_SUBDIRECTORY, which the user who
// created the output has. On NTFS the flush writes the directory's entries
// and commits the metadata log, so the creations and renames in it survive
// a power loss, as a directory fsync makes them on Unix. A filesystem that
// refuses the open or the flush returns its error.
func flushDir(d *os.File, sync func(faults.File) error) error {
	defer d.Close()
	oa := &windows.OBJECT_ATTRIBUTES{RootDirectory: windows.Handle(d.Fd()), ObjectName: &windows.NTUnicodeString{}}
	oa.Length = uint32(unsafe.Sizeof(*oa))
	var h windows.Handle
	err := windows.NtCreateFile(&h, windows.FILE_APPEND_DATA|windows.SYNCHRONIZE, oa, &windows.IO_STATUS_BLOCK{}, nil, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_OPEN_FOR_BACKUP_INTENT, 0, 0)
	if err != nil {
		var st windows.NTStatus
		if errors.As(err, &st) {
			err = st.Errno()
		}
		return &os.PathError{Op: "open to sync", Path: d.Name(), Err: err}
	}
	w := os.NewFile(uintptr(h), d.Name())
	return errors.Join(sync(w), w.Close())
}
