// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"fmt"
	"os"
	"strings"
)

// AnonymousConflict refuses --anonymous beside an explicit credential
// (credential names it: a flag, or the SAS in the source URL), which one
// of them would silently discard.
func AnonymousConflict(credential string) error {
	return Unsupported(fmt.Errorf("--anonymous excludes %s: anonymous requests carry no credentials", credential))
}

// AnonymousNote is the warning a provider gives once when --anonymous
// ignores credentials the environment holds: those of vars that are set,
// for provider (its title: S3, GCS, Azure Blob); "" when none is.
func AnonymousNote(provider string, vars ...string) string {
	var set []string
	for _, v := range vars {
		if os.Getenv(v) != "" {
			set = append(set, v)
		}
	}
	if len(set) == 0 {
		return ""
	}
	return "--anonymous: ignoring the " + provider + " credentials in the environment (" + strings.Join(set, ", ") + ")"
}
