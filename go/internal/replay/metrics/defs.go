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
	// HTTPRequestLatency is arrival to response written, injected delay
	// included.
	HTTPRequestLatency = Def{Name: "swath.replay.http.request.latency", Kind: Timer}
	// FixtureListLatency is one pager operation (store reads included).
	FixtureListLatency = Def{Name: "swath.replay.fixture.list.latency", Kind: Timer}
	// RequestLatency is the server's own cost per request shape, injected
	// delay excluded: read it against the injected profile for the same
	// shape.
	RequestLatency = Def{Name: "swath.replay.request.latency", Kind: Timer, Tags: []string{"shape"}}
	// RequestStageLatency splits a request into page, render, delay and
	// write.
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
	// ResponseWriteDeadline counts responses whose write outlived
	// --write-timeout.
	ResponseWriteDeadline = Def{Name: "swath.replay.response.write.deadline", Kind: Counter, Tags: []string{"protocol", "reason"}}
)
