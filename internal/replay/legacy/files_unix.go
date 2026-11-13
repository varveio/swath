// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package legacy

import (
	"fmt"
	"os"
	"syscall"
)

// DefaultMaxOpenFiles is half the process's soft descriptor limit, leaving
// the other half to the server's connections, the runtime and everything
// else, at most 4,096 (more open parts buy nothing: a read touches one part
// at a time) and at least one.
func DefaultMaxOpenFiles() int {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 32
	}
	return int(min(max(uint64(limit.Cur)/2, 1), 4096))
}

// OpenFlags are the flags a part is opened with: read-only and
// non-blocking, so opening a FIFO or a device put at a part's path returns
// at once instead of waiting for a writer; the caller then requires a
// regular file and calls Blocking.
const OpenFlags = os.O_RDONLY | syscall.O_NONBLOCK

// openRegular opens path with OpenFlags, requires a regular file and
// makes its descriptor blocking again.
func openRegular(path string) (*os.File, error) {
	f, err := os.OpenFile(path, OpenFlags, 0)
	if err != nil {
		return nil, err
	}
	if err := Blocking(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// Blocking requires f to be a regular file and clears the non-blocking
// flag OpenFlags set on it.
func Blocking(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s: %w", f.Name(), ErrNotRegular)
	}
	raw, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) { setErr = syscall.SetNonblock(int(fd), false) }); err != nil {
		return err
	}
	return setErr
}
