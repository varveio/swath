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
	cB2LongKeyRestarts
)

// clientCounters follow the range engine's in the S3 tally (the client's
// Counts.Store fills them), then the provider's (Stats fills them from a
// refreshed client).
var clientCounters = append(objhttp.CounterDefs("429/503/SlowDown"),
	counters.Def{Key: "credentials.refreshes", Kind: counters.Count, Unit: "refreshes", Help: "signing credentials fetched after the first: ahead of their expiry (by the smaller of 5 minutes and a quarter of their lifetime), or renewed; 0 for credentials that do not expire, --anonymous, a bearer token or a supplied client"},
	counters.Def{Key: "credentials.expired_refreshes", Kind: counters.Count, Unit: "refreshes", Help: "renewals fetched after an expired-token answer (ExpiredToken, ExpiredTokenException, TokenRefreshRequired) to credentials that can expire, its request retried once: one per generation of credentials, however many requests it refused"},
	counters.Def{Key: "b2.long_key_restarts", Kind: counters.Count, Unit: "pages", Help: "Backblaze B2 pages that ended on a key over 1,023 bytes, continued by start-after its longest prefix that B2 takes (B2 refuses its own continuation token there), the keys at or below the page's last dropped"})

// refreshed is implemented by clients that sign with refreshable
// credentials (client.Client).
type refreshed interface {
	CredentialRefreshes() (refreshes, forced int64)
}
