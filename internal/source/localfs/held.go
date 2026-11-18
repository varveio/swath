// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"fmt"
	"strings"

	"github.com/varveio/swath/reader/listing"
)

// LookupResult is what Opened.Lookup found at one key.
type LookupResult struct {
	// State is present, absent or unknown.
	State listing.LookupState
	// Row is the entry when State is present, keyed as a listing keys it:
	// a directory's key ends in "/".
	Row Row
	// Err says why the key was not reached when State is unknown: the
	// cause, beside its class in the listing contract (unreadReason).
	Err error
}

// keyNames splits key, below a held root (OpenDir), into its names: none
// for "", the root itself. A name is never empty, "." or "..", and holds
// no NUL. A prefix (OpenDir's) is "" or a directory key, ending in "/".
func keyNames(key string, prefix bool) ([]string, error) {
	if key == "" {
		return nil, nil
	}
	if prefix && !strings.HasSuffix(key, "/") {
		return nil, fmt.Errorf("localfs: prefix %q is not a directory key, ending in /", key)
	}
	names := strings.Split(strings.TrimSuffix(key, "/"), "/")
	for _, n := range names {
		if n == "" || n == "." || n == ".." || strings.IndexByte(n, 0) >= 0 {
			return nil, fmt.Errorf("localfs: key %q has a name that is empty, . or .., or holds NUL", key)
		}
	}
	return names, nil
}
