// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"os"
	"syscall"
)

func kill() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }
