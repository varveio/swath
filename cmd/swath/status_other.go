// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package main

import "os"

// statusSignals: none outside Unix.
var statusSignals []os.Signal
