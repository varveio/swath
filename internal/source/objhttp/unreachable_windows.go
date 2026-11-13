// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import "golang.org/x/sys/windows"

// unreachableErrnos are the connect errors Unreachable reads as
// deterministic: Winsock's codes, which syscall.ECONNREFUSED and its
// siblings (invented values on Windows) do not match.
var unreachableErrnos = []error{windows.WSAECONNREFUSED, windows.WSAENETUNREACH, windows.WSAEHOSTUNREACH}
