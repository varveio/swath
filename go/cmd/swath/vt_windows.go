// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableVT turns on virtual-terminal processing for stderr's console, so
// the redraw's escape sequences act; where it cannot (an old console), the
// progress records are appended as plain lines instead.
func enableVT() bool {
	h := windows.Handle(os.Stderr.Fd())
	var mode uint32
	if windows.GetConsoleMode(h, &mode) != nil {
		return false
	}
	return windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING) == nil
}
