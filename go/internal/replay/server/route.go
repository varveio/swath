// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/replay/javanum"
	"github.com/varveio/swath/go/internal/replay/latency"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/sortedparquet"
)

const protocolS3 = "s3"

// The replay's own answers on the S3 route, each with the reason sent in
// x-swath-replay-error.
var (
	errStopping    = &s3.Error{Status: 503, Code: "ServiceUnavailable", Message: "replay server is stopping", Reason: "shutdown"}
	errInterrupted = &s3.Error{Status: 503, Code: "ServiceUnavailable", Message: "replay request interrupted", Reason: "shutdown"}
	errCount       = &s3.Error{Status: 503, Code: "ServiceUnavailable", Message: output.ErrCount.Error(), Reason: output.ErrCount.Reason}
	errBudget      = &s3.Error{Status: 503, Code: "ServiceUnavailable", Message: output.ErrBudget.Error(), Reason: output.ErrBudget.Reason}
	errTooLarge    = &s3.Error{Status: 500, Code: "InternalError", Message: output.ErrTooLarge.Error(), Reason: output.ErrTooLarge.Reason}
	errInternal    = &s3.Error{Status: 500, Code: "InternalError", Message: "internal replay error", Reason: "internal"}
)

// requestTarget splits the request target as sent into its path and raw
// query: the S3 route compares the bucket with the path still
// percent-encoded, and reads the query's own bytes. An absolute target
// ("http://host/b?q") contributes the path after its authority.
func requestTarget(r *http.Request) (path, query string) {
	target := r.RequestURI
	path, _ = targetPath(target)
	if i := strings.IndexByte(target, '?'); i >= 0 {
		query = target[i+1:]
	}
	return path, query
}

// ServeHTTP answers the S3 route.
//
// A request holds two things for different spans. Its execution slot (the
// --max-concurrent-requests ceiling, a worker in a pool) covers parsing,
// paging, rendering and the injected delay, and is released before the
// response is written, so a client that reads slowly does not hold a
// worker. Its response (one of MaxResponses permits, and the budget bytes
// backing its chunks) is held from admission until the write completes.
// With N slots, up to 2N responses can be outstanding; the next request is
// refused with response_count_exhausted, not queued
// (TestSlowReadersReachTheResponseCeiling).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength != 0 {
		// Registered first, so it runs last: after the route's own
		// deferred work has given back the permit and the output.
		defer ignoreBody(w)()
	}
	if problem := targetProblem(r.RequestURI); problem != "" {
		rejectTarget(w, r, problem)
		return
	}
	arrived := time.Now()
	path, rawQuery := requestTarget(r)
	if s.serveOtherRoute(w, r, path, rawQuery) {
		return
	}
	// The request ceiling queues a request beyond it, as a full worker
	// pool would; shutdown releases the queue with a refusal. The slot is
	// given back as the answer starts, whatever the answer.
	release, ok := s.admit()
	defer release()
	rw := releasing(w, release)
	w = rw
	if !ok {
		s.refuse(w, r, path, arrived, errStopping)
		return
	}
	s.s3Active.Add(1)
	defer s.s3Active.Add(-1)

	req, err := s3.ParseHTTP(r.Method, path, rawQuery, s.cfg.Bucket)
	if err != nil {
		s.fail(w, r, path, arrived, err)
		return
	}
	if !s.tryResponse() {
		s.refuse(w, r, path, arrived, errCount)
		return
	}
	hold := &responseHold{s: s}
	defer hold.drop()
	rw.hold = hold
	// Reserve for a full page before reading it; the reservation shrinks
	// to what the page needs once it is rendered.
	out, err := output.New(s.budget, min(s.cfg.MaxResponseBytes, s3.EstimateSize(req.PageSize())), s.cfg.MaxResponseBytes)
	if err != nil {
		s.refuse(w, r, path, arrived, errBudget)
		return
	}
	hold.out = out

	// A client that leaves does not cancel its page: the page is read,
	// rendered and counted as served, and only its write fails, as the
	// replay wire contract's server does (TestClientGoneFinishesThePage).
	// Shutdown still cancels it.
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()
	defer context.AfterFunc(s.stopCtx, cancel)()

	started := time.Now()
	res, err := s.pager.List(ctx, req)
	s.reg.Get(metrics.FixtureListLatency).Record(time.Since(started))
	s.reg.Get(metrics.ServingPath, servingPathTag(s.cfg.ServingMode)).Inc()
	if err != nil {
		if s.stopCtx.Err() != nil {
			err = errInterrupted
		}
		s.fail(w, r, path, arrived, err)
		return
	}
	cost := time.Since(started)
	shape := latency.Classify(len(req.Delimiter) > 0, int64(req.MaxKeys))
	s.reg.Get(metrics.RequestLatency, shape.String()).Record(cost)
	s.reg.Get(metrics.RequestStageLatency, protocolS3, "page").Record(cost)
	s.logIfSlow(req, res, cost, shape)
	prefixes := res.CommonPrefixes()
	objects := len(res.Entries) - prefixes
	delay := s.cfg.Latency.Delay(shape, int64(prefixes), req.LatencyKey())

	renderStart := time.Now()
	out.SetFirstChunkHint(s3.EstimateSize(len(res.Entries)))
	s3.RenderList(out, res)
	if err := out.Err(); err != nil {
		e := errBudget
		if err == output.ErrTooLarge {
			e = errTooLarge
		}
		s.refuse(w, r, path, arrived, e)
		return
	}
	// Body returns the unused reservation: through the delay and the write
	// the response holds only its chunks.
	body := out.Body()
	s.reg.Get(metrics.RequestStageLatency, protocolS3, "render").Record(time.Since(renderStart))
	res = nil // the decoded page is not held through the delay and the write

	// The injected delay is the request's total target time: sleep only
	// for what the server's own work left of it.
	if delay > 0 {
		if remaining := delay - time.Since(started); remaining > 0 {
			delayStart := time.Now()
			t := time.NewTimer(remaining)
			select {
			case <-t.C:
			case <-s.stopCtx.Done():
				t.Stop()
				s.fail(w, r, path, arrived, errInterrupted)
				return
			}
			s.reg.Get(metrics.RequestStageLatency, protocolS3, "delay").Record(time.Since(delayStart))
		} else {
			s.reg.Get(metrics.InjectOverrun, shape.String()).Inc()
			s.reg.Get(metrics.InjectOverrunMs, shape.String()).Observe(float64(-remaining) / 1e6)
		}
	}

	neutral := neutralShape(shape)
	s.reg.Get(metrics.ProtocolObjects, protocolS3, neutral).Add(int64(objects))
	s.reg.Get(metrics.ProtocolPrefixes, protocolS3, neutral).Add(int64(prefixes))
	s.reg.Get(metrics.ProtocolEncodedBytes, protocolS3, neutral).Add(int64(out.Len()))
	onePass, fallback := out.PercentPaths()
	if onePass > 0 {
		s.reg.Get(metrics.ResponsePercentEncodingPath, protocolS3, "charged_chunk_one_pass").Add(int64(onePass))
	}
	if fallback > 0 {
		s.reg.Get(metrics.ResponsePercentEncodingPath, protocolS3, "exact_length_fallback").Add(int64(fallback))
	}
	release()
	s.write(w, r, 200, body, "", arrived)
}

// neutralShape is the provider-neutral spelling of a shape in the
// protocol.* meters.
func neutralShape(s latency.Shape) string {
	switch s {
	case latency.PivotProbe:
		return "seek"
	case latency.StructureProbe:
		return "delimiter"
	}
	return "page"
}

// fail answers an error: an S3 error as it is; a fixture found disordered
// inside a row group as a 500 fixture_disordered that names the part by
// its base name only (disorderError); anything else (a store failure) as a
// 500 that carries no detail. The full error, path included, goes to the
// log only.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, path string, arrived time.Time, err error) {
	var e *s3.Error
	var disorder *sortedparquet.DisorderError
	switch {
	case errors.As(err, &e):
	case errors.As(err, &disorder):
		fmt.Fprintf(s.cfg.Log, "replay refused a request over a disordered fixture: %v\n", err)
		e = disorderError(disorder)
	default:
		fmt.Fprintf(s.cfg.Log, "replay failed to serve request: %v\n", err)
		e = errInternal
	}
	s.writeError(w, r, path, arrived, e)
}

// disorderError is the replay wire contract's answer to a row group whose
// keys do not ascend: the reason row_group_disorder, the group and the
// part's file name, never the server's directory layout
// (TestDisorderedFixtureNamesOnlyThePart).
func disorderError(d *sortedparquet.DisorderError) *s3.Error {
	msg := fmt.Sprintf("row group %d of %s is not sorted (reason=%s): %s",
		d.RowGroup, filepath.Base(d.Part), sortedparquet.ReasonRowGroupDisorder, d.Detail)
	return &s3.Error{Status: 500, Code: "InternalError", Message: msg, Reason: "fixture_disordered"}
}

// refuse answers an admission refusal and counts it.
func (s *Server) refuse(w http.ResponseWriter, r *http.Request, path string, arrived time.Time, e *s3.Error) {
	s.reg.Get(metrics.ResponseAdmissionRefused, protocolS3, e.Reason).Inc()
	s.writeError(w, r, path, arrived, e)
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, path string, arrived time.Time, e *s3.Error) {
	dropResponse(w)
	s.write(w, r, e.Status, [][]byte{s3.AppendError(nil, e, path)}, e.Reason, arrived)
}

// write sends a response: the replay wire contract's headers (lower-case
// names, as sent), the body's views in writes of at most OutputChunkBytes,
// and a write deadline that starts when writing does, so an injected delay
// never counts against it.
func (s *Server) write(w http.ResponseWriter, r *http.Request, status int, body [][]byte, reason string, arrived time.Time) {
	h := w.Header()
	h["Content-Type"] = []string{"application/xml"}
	h["Content-Length"] = []string{strconv.Itoa(viewsLen(body))}
	h["x-amz-request-id"] = []string{"S3LISTINGREPLAY"}
	h["x-amz-id-2"] = []string{"S3LISTINGREPLAY"}
	if reason != "" {
		h["x-swath-replay-error"] = []string{reason}
	}
	s.reg.Get(metrics.HTTPRequests).Inc()
	if status >= 500 {
		s.reg.Get(metrics.HTTPErrors).Inc()
	}
	s.reg.Get(metrics.ProtocolHTTPRequests, protocolS3, strconv.Itoa(status/100)+"xx").Inc()
	// The request latency ends where the write begins, and only a page
	// has a write stage: the replay wire contract's meter semantics
	// (TestRequestMeterSpans).
	s.reg.Get(metrics.HTTPRequestLatency).Record(time.Since(arrived))
	writeStart := time.Now()
	_ = http.NewResponseController(w).SetWriteDeadline(writeStart.Add(s.cfg.WriteTimeout))
	w.WriteHeader(status)
	if err := sendBody(w, r, body); errors.Is(err, os.ErrDeadlineExceeded) {
		s.reg.Get(metrics.ResponseWriteDeadline, protocolS3, "total_deadline").Inc()
	}
	if status/100 == 2 {
		s.reg.Get(metrics.RequestStageLatency, protocolS3, "write").Record(time.Since(writeStart))
	}
}

// viewsLen is a body's length.
func viewsLen(body [][]byte) int {
	n := 0
	for _, v := range body {
		n += len(v)
	}
	return n
}

// ignoreBody answers a request that declares a body (a Content-Length or
// chunked) without reading it: the listing routes take none. The answer
// is the one the request would get without a body, as the contract's
// server gives it, and the connection is closed after it rather than
// drained, which is also what the contract's server does. Two things
// stop net/http from reading a body that may never arrive: Connection:
// close on the answer, which skips the drain before the answer is
// committed (inside the handler, while the response permit is held), and
// an immediate read deadline, set as the handler returns, which ends the
// read net/http makes when it closes the body afterwards
// (TestUnsentBodyHoldsNothing).
func ignoreBody(w http.ResponseWriter) (done func()) {
	w.Header()["Connection"] = []string{"close"}
	return func() { _ = http.NewResponseController(w).SetReadDeadline(time.Now()) }
}

// releasingWriter gives back a request's execution slot and store
// registration when its answer starts, so no answer, refusals included,
// is written while holding a slot: the contract's workers hand every
// answer to an asynchronous write. A response permit, where the request
// holds one, stays held until the handler returns, after the flush.
type releasingWriter struct {
	http.ResponseWriter
	release func()
	hold    *responseHold
}

func releasing(w http.ResponseWriter, release func()) *releasingWriter {
	return &releasingWriter{ResponseWriter: w, release: release}
}

// responseHold is a request's response permit and, once reserved, its
// output. A successful answer drops it when the handler returns, after the
// flush; an error answer drops it before it is written (dropResponse), as
// the contract closes the output and returns the permit before sending its
// small, unbudgeted error.
type responseHold struct {
	s       *Server
	out     *output.Output
	dropped bool
}

func (h *responseHold) drop() {
	if h.dropped {
		return
	}
	h.dropped = true
	if h.out != nil {
		h.out.Close()
	}
	h.s.responses.Add(-1)
}

// dropResponse drops the response hold of the request w answers, if it
// has one: an error answer holds neither permit nor output.
func dropResponse(w http.ResponseWriter) {
	if rw, ok := w.(*releasingWriter); ok && rw.hold != nil {
		rw.hold.drop()
	}
}

func (w *releasingWriter) WriteHeader(code int) {
	w.release()
	w.ResponseWriter.WriteHeader(code)
}

func (w *releasingWriter) Write(b []byte) (int, error) {
	w.release()
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the connection's deadline
// and flush.
func (w *releasingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// sendBody writes a body (none for HEAD) and then flushes it, all under
// the write deadline the caller set. The flush is what makes the response
// ownership real: small writes only copy into the server's buffers, and
// without it the final flush would happen after the handler returned and
// released the response permit, so a client that stops reading could
// hold buffered responses the ceiling no longer counts
// (TestPipelinedSmallResponsesStayWithinTheCeiling). A flush error is the
// write's error.
func sendBody(w http.ResponseWriter, r *http.Request, body [][]byte) error {
	if r.Method != http.MethodHead {
		if err := writeViews(w, body); err != nil {
			return err
		}
	}
	return http.NewResponseController(w).Flush()
}

// writeViews writes a body's views in writes of at most OutputChunkBytes,
// stopping at the first error.
func writeViews(w http.ResponseWriter, body [][]byte) error {
	for _, v := range body {
		for len(v) > 0 {
			n := min(len(v), OutputChunkBytes)
			if _, err := w.Write(v[:n]); err != nil {
				return err
			}
			v = v[n:]
		}
	}
	return nil
}

// logIfSlow logs a request whose own cost reached the slow-request
// threshold.
func (s *Server) logIfSlow(req *s3.Request, res *s3.Result, cost time.Duration, shape latency.Shape) {
	if s.cfg.SlowRequestLog < 0 || cost < s.cfg.SlowRequestLog {
		return
	}
	cont := "none"
	if req.TokenSet {
		cont = "yes"
	}
	fmt.Fprintf(s.cfg.Log, "replay slow request shape=%s ms=%s prefix=%s delimiter=%s start_after=%s continuation=%s max_keys=%d entries=%d truncated=%v\n",
		shape.Upper(), javanum.FormatDouble(float64(cost.Nanoseconds())/1e6), logBytes(req.Prefix),
		logBytes(req.Delimiter), logBytes(req.StartAfter), cont, req.MaxKeys, len(res.Entries), res.Truncated)
}

// logBytes shows key bytes one character per byte (Latin-1), "-" when
// absent, so a log line never splits a malformed key.
func logBytes(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	r := make([]rune, len(b))
	for i, c := range b {
		r[i] = rune(c)
	}
	return string(r)
}
