// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package evidence holds the provider evidence replay: tests that replay
// every native capture in the committed evidence archive
// (docs/replay-provider-evidence.tar.gz) against the Go replay server
// serving the same corpus, compare each answer with the provider's under
// the conformance field policy, and hold each outcome to the archive's
// pin or better. The package has no code of its own outside its tests.
package evidence
