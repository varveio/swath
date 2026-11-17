// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package swath

import "github.com/varveio/swath/internal/live"

// Source is an opened live location, opened by a provider package such as
// github.com/varveio/swath/s3. It answers Page, Lookup, Walk and
// Summarize as the package documentation states, and Close releases what
// it holds. It is safe for concurrent use. The zero Source is not usable.
type Source = live.Source
