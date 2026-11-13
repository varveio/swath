// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync/atomic"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source/objhttp"
)

// The client's own counters' offsets in CounterDefs, after objhttp's:
// transport.pool_size, retry_after_capped and list_format.* (Formats).
const (
	CPoolSize = int(objhttp.NumCounters) + iota
	CRetryAfterCapped
	CArrowPages
	CArrowItems
	CXMLPages
	CXMLItems
)

// CounterDefs are the client's counters, as GCS declares its own: objhttp's
// (attempts, retries, throttles, timeouts, parse_failures and the
// admission's; Counters().Store fills them), then transport.pool_size
// (Transports), retry_after_capped (RetryAfterCapped) and the pages and
// items each listing format carried (Formats).
var CounterDefs = append(objhttp.CounterDefs("503 ServerBusy and 429"),
	counters.Def{Key: "transport.pool_size", Kind: counters.Max, Unit: "transports", Help: "HTTP transports the built client spreads requests over, round-robin: one per 16 workers, 1 to 8 (one HTTP/2 connection each below the server's stream limit); 0 with a supplied client that does not report one"},
	counters.Def{Key: "retry_after_capped", Kind: counters.Count, Unit: "answers", Help: "answers whose Retry-After exceeded the request timeout: the wait before the next attempt was cut to it"},
	counters.Def{Key: "list_format.arrow_pages", Kind: counters.Count, Unit: "pages", Help: "listing pages the service answered in Arrow, the format every listing asks for first"},
	counters.Def{Key: "list_format.arrow_items", Kind: counters.Count, Unit: "items", Help: "blobs and prefixes on the Arrow pages"},
	counters.Def{Key: "list_format.xml_pages", Kind: counters.Count, Unit: "pages", Help: "listing pages the service answered in XML though Arrow was asked: an account or endpoint without Arrow listings (a hierarchical namespace, Azurite, swath-replay)"},
	counters.Def{Key: "list_format.xml_items", Kind: counters.Count, Unit: "items", Help: "blobs and prefixes on the XML pages"})

// Formats is how many valid listing pages, and their items, each response
// format carried (list_format.*).
type Formats struct{ ArrowPages, ArrowItems, XMLPages, XMLItems int64 }

// formats is the client's running Formats.
type formats struct{ arrowPages, arrowItems, xmlPages, xmlItems atomic.Int64 }

func (f *formats) count(arrow bool, items int) {
	if arrow {
		f.arrowPages.Add(1)
		f.arrowItems.Add(int64(items))
		return
	}
	f.xmlPages.Add(1)
	f.xmlItems.Add(int64(items))
}

// Formats snapshots the pages and items each listing format carried.
func (c *Client) Formats() Formats {
	f := &c.formats
	return Formats{f.arrowPages.Load(), f.arrowItems.Load(), f.xmlPages.Load(), f.xmlItems.Load()}
}
