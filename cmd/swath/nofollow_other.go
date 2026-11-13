// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package main

// oNoFollow: no such open flag here; createNoFollow's Lstat refuses a link.
const oNoFollow = 0
