// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !swathtest

package main

// testHooks installs nothing: the SWATH_TEST_* hooks exist only in a build
// with the swathtest tag (testhooks.go).
func testHooks() {}
