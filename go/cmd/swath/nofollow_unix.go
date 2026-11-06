// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package main

import "syscall"

// oNoFollow makes an open of a symbolic link fail (ELOOP).
const oNoFollow = syscall.O_NOFOLLOW
