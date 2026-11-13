// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"time"

	"github.com/varveio/swath/internal/source/tokensource"
)

// Bearer-token authentication (--s3-bearer-token-command), as Java swath's:
// a shell command prints an OAuth bearer token on stdout, sent as
// "Authorization: Bearer TOKEN" instead of a SigV4 signature on every
// request (GCS's XML API with `gcloud auth print-access-token`, or any
// S3-compatible service whose credentials are bearer tokens). The token is
// fetched again after the refresh interval (default 45 minutes), one
// refresh at a time (tokensource.Shared); a command that fails, times out
// or prints nothing fails the request (class auth): a stale token is never
// reused. Neither the command nor the token is written anywhere.

// NewBearerToken is the token source for command, refreshed every refresh
// (0: tokensource.DefaultRefresh).
func NewBearerToken(command string, refresh time.Duration) *tokensource.Shared {
	return tokensource.NewShared(&tokensource.Command{Cmd: command, Refresh: refresh})
}
