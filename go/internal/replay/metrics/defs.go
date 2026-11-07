// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package metrics

// The replay server's meters. Names and tags are the ones the Java server
// publishes, so a scrape reads the same with either server behind it; the
// bench scripts sum swath.replay.http.requests and read
// swath.replay.request.latency per shape.
var (
	// HTTPRequests counts every answered request on the serving port,
	// errors included.
	HTTPRequests = Def{Name: "swath.replay.http.requests", Kind: Counter}
	// HTTPErrors counts answers with a 5xx status.
	HTTPErrors = Def{Name: "swath.replay.http.errors", Kind: Counter}
	// HTTPRequestLatency is arrival to the start of the response's write,
	// injected delay included.
	HTTPRequestLatency = Def{Name: "swath.replay.http.request.latency", Kind: Timer}
	// FixtureListLatency is one pager operation (store reads included).
	FixtureListLatency = Def{Name: "swath.replay.fixture.list.latency", Kind: Timer}
	// RequestLatency is the server's own cost per request shape, injected
	// delay excluded: read it against the injected profile for the same
	// shape.
	RequestLatency = Def{Name: "swath.replay.request.latency", Kind: Timer, Tags: []string{"shape"}}
	// RequestStageLatency splits a page into page, render, delay and
	// write; an error answer has no write stage.
	RequestStageLatency = Def{Name: "swath.replay.request.stage.latency", Kind: Timer, Tags: []string{"protocol", "stage"}}
	// ServingPath counts pager operations by serving mode.
	ServingPath = Def{Name: "swath.replay.serving.path", Kind: Counter, Tags: []string{"mode"}}
	// DelimiterPath counts delimiter pages by the path that served them:
	// the store's rollup or the pager's range walk.
	DelimiterPath = Def{Name: "swath.replay.delimiter.path", Kind: Counter, Tags: []string{"path"}}
	// InjectOverrun counts requests whose own cost exceeded their injected
	// target; InjectOverrunMs records by how much.
	InjectOverrun   = Def{Name: "swath.replay.inject.overrun", Kind: Counter, Tags: []string{"shape"}}
	InjectOverrunMs = Def{Name: "swath.replay.inject.overrun.ms", Kind: Distribution, Tags: []string{"shape"}}
	// ProtocolHTTPRequests counts answers by protocol and status class.
	ProtocolHTTPRequests = Def{Name: "swath.replay.protocol.http.requests", Kind: Counter, Tags: []string{"protocol", "status_class"}}
	// ProtocolObjects, ProtocolPrefixes and ProtocolEncodedBytes count
	// successful page output by protocol and neutral shape (page, seek,
	// delimiter).
	ProtocolObjects      = Def{Name: "swath.replay.protocol.objects", Kind: Counter, Tags: []string{"protocol", "shape"}}
	ProtocolPrefixes     = Def{Name: "swath.replay.protocol.prefixes", Kind: Counter, Tags: []string{"protocol", "shape"}}
	ProtocolEncodedBytes = Def{Name: "swath.replay.protocol.encoded.bytes", Kind: Counter, Tags: []string{"protocol", "shape"}}
	// ProtocolRequestsActive is the requests in progress per protocol.
	ProtocolRequestsActive = Def{Name: "swath.replay.protocol.requests.active", Kind: Gauge, Tags: []string{"protocol"}}
	// ResponseAdmissionRefused counts refusals by reason: shutdown,
	// response_count_exhausted, response_budget_exhausted,
	// response_too_large.
	ResponseAdmissionRefused = Def{Name: "swath.replay.response.admission.refused", Kind: Counter, Tags: []string{"protocol", "reason"}}
	// ResponseBytesLive and ResponseBytesPeak are the encoded response
	// bytes charged to the budget now and at most; ResponseActive the
	// responses holding a permit.
	ResponseBytesLive = Def{Name: "swath.replay.response.bytes.live", Kind: Gauge}
	ResponseBytesPeak = Def{Name: "swath.replay.response.bytes.peak", Kind: Gauge}
	ResponseActive    = Def{Name: "swath.replay.response.active", Kind: Gauge}
	// ResponseChunkAllocation counts response chunks by allocation reason
	// (initial, initial_cap_partial, continuation, cap_partial), and
	// ResponseChunkCapacityBytes records their sizes.
	ResponseChunkAllocation    = Def{Name: "swath.replay.response.chunk.allocation", Kind: Counter, Tags: []string{"reason"}}
	ResponseChunkCapacityBytes = Def{Name: "swath.replay.response.chunk.capacity.bytes", Kind: Distribution, Tags: []string{"reason"}}
	// ResponsePercentEncodingPath counts encoding-type=url values by how
	// they were written: charged_chunk_one_pass into room already charged,
	// or exact_length_fallback after admitting their exact length.
	ResponsePercentEncodingPath = Def{Name: "swath.replay.response.percent.encoding.path", Kind: Counter, Tags: []string{"protocol", "reason"}}
	// ResponseWriteDeadline counts responses whose write outlived
	// --write-timeout.
	ResponseWriteDeadline = Def{Name: "swath.replay.response.write.deadline", Kind: Counter, Tags: []string{"protocol", "reason"}}
)

// The fixture and store meters.
var (
	// ProviderPath counts native pager decisions on the GCS and Azure
	// routes: the path engaged and why.
	ProviderPath = Def{Name: "swath.replay.provider.path", Kind: Counter, Tags: []string{"protocol", "path", "reason"}}
	// ServingRefused counts requests refused at read time for a fixture
	// reason; row_group_disorder is a row group whose keys do not ascend.
	ServingRefused = Def{Name: "swath.replay.serving.refused", Kind: Counter, Tags: []string{"reason"}}
	// IndexLoadLatency is the fixture open (footers read, row-group first
	// keys decoded, the routing index built), once at startup;
	// IndexEntries its routing entries, one per non-empty row group.
	IndexLoadLatency = Def{Name: "swath.replay.index.load.latency", Kind: Timer, Tags: []string{"source"}, NoPercentiles: true}
	IndexEntries     = Def{Name: "swath.replay.index.entries", Kind: Distribution}
	// PageReadLatency is the time a store read holds its reader slot: the
	// decode service time, slot wait excluded.
	PageReadLatency = Def{Name: "swath.replay.page.read.latency", Kind: Timer}
	// ParquetQueryLatency is one store call, slot wait included;
	// ParquetQueryRows the rows (or rollup entries) it returned and
	// ParquetQueryErrors the calls that failed.
	ParquetQueryLatency = Def{Name: "swath.replay.parquet.query.latency", Kind: Timer}
	ParquetQueryRows    = Def{Name: "swath.replay.parquet.query.rows", Kind: Distribution}
	ParquetQueryErrors  = Def{Name: "swath.replay.parquet.query.errors", Kind: Counter}
	// ParquetQueriesInFlight and ParquetQueriesPeak are the reads holding
	// a reader slot (--parquet-connections) now and at most.
	ParquetQueriesInFlight = Def{Name: "swath.replay.parquet.queries.in_flight", Kind: Gauge}
	ParquetQueriesPeak     = Def{Name: "swath.replay.parquet.queries.peak", Kind: Gauge}
	// The "/" skip-scan's work: key cursors opened on a row group (a group
	// can be opened again after a reseek), row groups emitted as one
	// common prefix from routing keys alone, and per rollup the key values
	// decoded and the cursor moves through the page index.
	DelimiterSkipScanRowGroupOpens       = Def{Name: "swath.replay.delimiter.skipscan.row_group_opens", Kind: Counter}
	DelimiterSkipScanWholeGroupShortcuts = Def{Name: "swath.replay.delimiter.skipscan.whole_group_shortcuts", Kind: Counter}
	DelimiterSkipScanDecodedKeyRows      = Def{Name: "swath.replay.delimiter.skipscan.decoded_key_rows", Kind: Distribution}
	DelimiterSkipScanPageReseeks         = Def{Name: "swath.replay.delimiter.skipscan.page_reseeks", Kind: Distribution}
	// The descriptor cache every part is read through: files opened
	// (first opens and reopens after eviction), idle descriptors closed to
	// make room, reads that waited for a descriptor, and the descriptors
	// open now, at most, and allowed.
	HandleCacheOpens     = Def{Name: "swath.replay.handle_cache.opens", Kind: Counter}
	HandleCacheEvictions = Def{Name: "swath.replay.handle_cache.evictions", Kind: Counter}
	HandleCacheWaits     = Def{Name: "swath.replay.handle_cache.waits", Kind: Counter}
	HandleCacheOpen      = Def{Name: "swath.replay.handle_cache.open", Kind: Gauge}
	HandleCachePeak      = Def{Name: "swath.replay.handle_cache.peak", Kind: Gauge}
	HandleCacheLimit     = Def{Name: "swath.replay.handle_cache.limit", Kind: Gauge}
	// The window cache in front of range reads (store.Prefetch): reads
	// answered from windows, misses by why their fill was sized as it was
	// (continuation, cold), each fill's duration and the rows it asked
	// for, continuation anchors (registered, claimed,
	// evicted_before_claim) and the ramp ceiling of a claimed one, windows
	// evicted (and those the row bound forced), how each read was served
	// (selection), why a two-window join was declined or hit, reads and
	// windows kept out of the cache (uncached), and the windows, anchors
	// and rows held now.
	PrefetchWindowHit               = Def{Name: "swath.replay.prefetch.window.hit", Kind: Counter}
	PrefetchWindowMiss              = Def{Name: "swath.replay.prefetch.window.miss", Kind: Counter, Tags: []string{"reason"}}
	PrefetchWindowFill              = Def{Name: "swath.replay.prefetch.window.fill", Kind: Timer}
	PrefetchFillRows                = Def{Name: "swath.replay.prefetch.fill.rows", Kind: Distribution}
	PrefetchAnchor                  = Def{Name: "swath.replay.prefetch.anchor", Kind: Counter, Tags: []string{"event"}}
	PrefetchWindowRampCeilingRows   = Def{Name: "swath.replay.prefetch.window.ramp_ceiling_rows", Kind: Distribution}
	PrefetchWindowEviction          = Def{Name: "swath.replay.prefetch.window.eviction", Kind: Counter}
	PrefetchWindowRowBudgetEviction = Def{Name: "swath.replay.prefetch.window.row_budget_eviction", Kind: Counter}
	PrefetchWindowSelection         = Def{Name: "swath.replay.prefetch.window.selection", Kind: Counter, Tags: []string{"reason"}}
	PrefetchWindowJoin              = Def{Name: "swath.replay.prefetch.window.join", Kind: Counter, Tags: []string{"reason"}}
	PrefetchWindowUncached          = Def{Name: "swath.replay.prefetch.window.uncached", Kind: Counter, Tags: []string{"reason"}}
	PrefetchWindowsLive             = Def{Name: "swath.replay.prefetch.windows.live", Kind: Gauge}
	PrefetchAnchorsLive             = Def{Name: "swath.replay.prefetch.anchors.live", Kind: Gauge}
	PrefetchRowsLive                = Def{Name: "swath.replay.prefetch.rows.live", Kind: Gauge}
	// SortFixtureBuildLatency and SortFixtureOutputBytes are the
	// sort-fixture command's; a server never sorts, so they read zero.
	SortFixtureBuildLatency = Def{Name: "swath.replay.sortfixture.build.latency", Kind: Timer, NoPercentiles: true}
	SortFixtureOutputBytes  = Def{Name: "swath.replay.sortfixture.output.bytes", Kind: Distribution}
)
