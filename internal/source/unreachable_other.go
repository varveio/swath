// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package source

import "syscall"

// unreachableErrnos are the connect errors Unreachable reads as
// deterministic.
var unreachableErrnos = []error{syscall.ECONNREFUSED, syscall.ENETUNREACH, syscall.EHOSTUNREACH}
