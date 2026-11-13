// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import "os"

func kill() { os.Exit(137) }

// exit ends the process with code: Windows has no signal to die by, so a
// stopped run exits with its code (130, 143).
func exit(code int) { os.Exit(code) }
