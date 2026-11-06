// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package localfs

import "syscall"

func mkfifo(p string) error { return syscall.Mkfifo(p, 0o644) }
