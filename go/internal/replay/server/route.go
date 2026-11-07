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
// percent-encoded, and reads the query's own bytes.
func requestTarget(r *http.Request) (path, query string) {
	target := r.RequestURI
	if !strings.HasPrefix(target, "/") {
		return r.URL.EscapedPath(), r.URL.RawQuery
	}
	path, query, _ = strings.Cut(target, "?")
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
	arrived := time.Now()
	path, rawQuery := requestTarget(r)
	// The request ceiling queues a request beyond it, as a full worker
	// pool would; shutdown releases the queue with a refusal.
	slot := false
	select {
	case s.slots <- struct{}{}:
		slot = true
	case <-s.stopCtx.Done():
	}
	releaseSlot := func() {
		if slot {
			slot = false
			<-s.slots
		}
	}
	defer releaseSlot()
	if !s.begin() {
		s.refuse(w, r, path, arrived, errStopping)
		return
	}
	operating := true
	endOperation := func() {
		if operating {
			operating = false
			s.end()
		}
	}
	defer endOperation()
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
	defer s.responses.Add(-1)
	// Reserve for a full page before reading it; the reservation shrinks
	// to what the page needs once it is rendered.
	out, err := output.New(s.budget, min(s.cfg.MaxResponseBytes, s3.EstimateSize(req.PageSize())), s.cfg.MaxResponseBytes)
	if err != nil {
		s.refuse(w, r, path, arrived, errBudget)
		return
	}
	defer out.Close()

	ctx, cancel := context.WithCancel(r.Context())
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
	endOperation()
	releaseSlot()
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
	s.write(w, r, e.Status, [][]byte{s3.AppendError(nil, e, path)}, e.Reason, arrived)
}

// write sends a response: the replay wire contract's headers (lower-case
// names, as sent), the body's views in writes of at most OutputChunkBytes,
// and a write deadline that starts when writing does, so an injected delay
// never counts against it.
func (s *Server) write(w http.ResponseWriter, r *http.Request, status int, body [][]byte, reason string, arrived time.Time) {
	n := 0
	for _, v := range body {
		n += len(v)
	}
	h := w.Header()
	h["Content-Type"] = []string{"application/xml"}
	h["Content-Length"] = []string{strconv.Itoa(n)}
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
	writeStart := time.Now()
	_ = http.NewResponseController(w).SetWriteDeadline(writeStart.Add(s.cfg.WriteTimeout))
	w.WriteHeader(status)
	var err error
	if r.Method != http.MethodHead {
		for _, v := range body {
			for len(v) > 0 && err == nil {
				n := min(len(v), OutputChunkBytes)
				_, err = w.Write(v[:n])
				v = v[n:]
			}
		}
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		s.reg.Get(metrics.ResponseWriteDeadline, protocolS3, "total_deadline").Inc()
	}
	s.reg.Get(metrics.RequestStageLatency, protocolS3, "write").Record(time.Since(writeStart))
	s.reg.Get(metrics.HTTPRequestLatency).Record(time.Since(arrived))
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
		shape.Upper(), strconv.FormatFloat(float64(cost.Nanoseconds())/1e6, 'f', -1, 64), logBytes(req.Prefix),
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
