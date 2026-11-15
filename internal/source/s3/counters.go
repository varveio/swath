// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source/objhttp"
)

// The provider's counters, after the client's.
const (
	cCredentialRefreshes = int(objhttp.NumCounters) + iota
	cCredentialExpired
)

// clientCounters follow the range engine's in the S3 tally (the client's
// Counts.Store fills them), then the provider's (Stats fills them from a
// refreshed client).
var clientCounters = append(objhttp.CounterDefs("429/503/SlowDown"),
	counters.Def{Key: "credentials.refreshes", Kind: counters.Count, Unit: "refreshes", Help: "signing credentials refreshed after the first lookup and taken into use: 5 minutes before they expire, or forced; 0 for credentials that do not expire, --anonymous, a bearer token or a supplied client"},
	counters.Def{Key: "credentials.expired_refreshes", Kind: counters.Count, Unit: "refreshes", Help: "refreshes forced by an expired-token answer (ExpiredToken, ExpiredTokenException, TokenRefreshRequired) to refreshable credentials, its request retried once; the answers to requests signed together force one"})

// refreshed is implemented by clients that sign with refreshable
// credentials (client.Client).
type refreshed interface {
	CredentialRefreshes() (refreshes, forced int64)
}
