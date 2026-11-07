// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/replay/gcs"
	"github.com/varveio/swath/go/internal/replay/latency"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
)

// The listing routes --protocols selects, and the profile each serves.
const (
	ProtocolS3  = "s3"
	ProtocolGCS = "gcs"
	// protocolReplay tags the replay's own answers to a request no
	// enabled route claims.
	protocolReplay = "replay"
)

var profiles = map[string]string{ProtocolS3: "list-objects-v2", ProtocolGCS: "json-v1"}

// gcsPathPrefix is the path every GCS JSON API request starts with; the
// route answers /storage/v1/b/{bucket}/o and refuses the rest as an
// unknown bucket.
const gcsPathPrefix = "/storage/v1/b/"

// ProviderPath counts the path a provider route took for a page or a
// refusal, and why (the GCS route's flat, delimiter_indexed,
// delimiter_walk, query_refused, ...).
var ProviderPath = metrics.Def{Name: "swath.replay.provider.path", Kind: metrics.Counter, Tags: []string{"protocol", "path", "reason"}}

// routes is the multi-protocol state of a server; nil when only the S3
// route is enabled, so the S3 route answers every request as before.
type routes struct {
	s3        bool
	gcs       *gcs.Pager
	gcsActive atomic.Int64
}

// ValidateProtocols checks a --protocols selection against the bucket: a
// non-empty subset of s3 and gcs, a bucket name a GCS client can address
// when gcs is enabled, and not the bucket "storage" with both enabled
// (its S3 paths would be GCS paths).
func ValidateProtocols(protocols []string, bucket string) error {
	if len(protocols) == 0 {
		return errors.New("--protocols must name at least one protocol")
	}
	for _, p := range protocols {
		if _, ok := profiles[p]; !ok {
			return fmt.Errorf("--protocols: unknown or unbuilt protocol %q", p)
		}
	}
	if slices.Contains(protocols, ProtocolS3) && slices.Contains(protocols, ProtocolGCS) && bucket == "storage" {
		return errors.New("S3 bucket storage conflicts with enabled GCS route")
	}
	if slices.Contains(protocols, ProtocolGCS) && !validGCSBucket(bucket) {
		return errors.New("bucket cannot be exposed as a GCS bucket")
	}
	return nil
}

var (
	gcsBucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*[a-z0-9]$`)
	ipv4Part      = regexp.MustCompile(`^[0-9]{1,3}$`)
)

// validGCSBucket is the bucket naming rule: 3 to 63 characters (222 with
// dots, each dot-separated label at most 63), lower-case letters, digits,
// '-', '_' and '.', beginning and ending with a letter or digit, not an
// IPv4 address, not starting with "goog" and not containing "google" or a
// look-alike (0 for o, 1 for l).
func validGCSBucket(name string) bool {
	limit := 63
	if strings.Contains(name, ".") {
		limit = 222
	}
	if len(name) < 3 || len(name) > limit || !gcsBucketName.MatchString(name) || strings.HasPrefix(name, "goog") {
		return false
	}
	if strings.Contains(strings.NewReplacer("0", "o", "1", "l").Replace(name), "google") {
		return false
	}
	labels := strings.Split(name, ".")
	for _, l := range labels {
		if l == "" || len(l) > 63 {
			return false
		}
	}
	if len(labels) == 4 {
		for _, l := range labels {
			if n, err := strconv.Atoi(l); !ipv4Part.MatchString(l) || err != nil || n > 255 {
				return true
			}
		}
		return false
	}
	return true
}

// protocols is the enabled routes, sorted: the metrics payload's and the
// startup line's spelling.
func (c *Config) protocols() []string {
	if len(c.Protocols) == 0 {
		return []string{ProtocolS3}
	}
	p := slices.Clone(c.Protocols)
	slices.Sort(p)
	return slices.Compact(p)
}

// ProfilesString is the startup line's profiles value, {gcs=json-v1,
// s3=list-objects-v2}.
func (c *Config) ProfilesString() string {
	var parts []string
	for _, p := range c.protocols() {
		parts = append(parts, p+"="+profiles[p])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// ProtocolList is the startup line's protocols value: the enabled routes,
// sorted, comma-separated.
func (c *Config) ProtocolList() string { return strings.Join(c.protocols(), ",") }

// appendServingProtocols writes the metrics payload's "protocols" array.
func (c *Config) appendServingProtocols(dst []byte) []byte {
	dst = append(dst, '[')
	for i, p := range c.protocols() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = metrics.AppendString(dst, p)
	}
	return append(dst, ']')
}

// appendServingProfiles writes the metrics payload's "profiles" object.
func (c *Config) appendServingProfiles(dst []byte) []byte {
	dst = append(dst, '{')
	for i, p := range c.protocols() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = metrics.AppendString(dst, p)
		dst = append(dst, ':')
		dst = metrics.AppendString(dst, profiles[p])
	}
	return append(dst, '}')
}

// initRoutes sets up the routes other than S3's when --protocols enables
// them.
func (s *Server) initRoutes() {
	ps := s.cfg.protocols()
	if len(ps) == 1 && ps[0] == ProtocolS3 {
		return
	}
	rt := &routes{s3: slices.Contains(ps, ProtocolS3)}
	if slices.Contains(ps, ProtocolGCS) {
		rt.gcs = gcs.NewPager(s.cfg.Store, s.cfg.FixtureIdentity)
		rt.gcs.OnPath = func(path, reason string) { s.reg.Get(ProviderPath, ProtocolGCS, path, reason).Inc() }
		s.reg.SetGauge(metrics.ProtocolRequestsActive, func() float64 { return float64(rt.gcsActive.Load()) }, ProtocolGCS)
	}
	s.routes = rt
}

// serveOtherRoute answers a request that belongs to a route other than
// S3's, and reports whether it did. With several routes enabled a request
// is classified before anything else: a /storage/v1/b/ path is the GCS
// route's (or ambiguous, with an S3 list-type parameter); anything else
// is the S3 route's when it is enabled and otherwise unclaimed.
func (s *Server) serveOtherRoute(w http.ResponseWriter, r *http.Request, path, rawQuery string) bool {
	rt := s.routes
	if rt == nil {
		return false
	}
	if rt.gcs != nil && strings.HasPrefix(path, gcsPathPrefix) {
		if hasQueryKey(rawQuery, "list-type") {
			s.serveEnvelope(w, r, 400, "replay_ambiguous_request")
		} else {
			s.serveGCS(w, r, path, rawQuery)
		}
		return true
	}
	if s.azureClaims(path, rawQuery) {
		if hasQueryKey(rawQuery, "list-type") {
			s.serveEnvelope(w, r, 400, "replay_ambiguous_request")
		} else {
			s.serveAzure(w, r, path, rawQuery, time.Now())
		}
		return true
	}
	if rt.s3 {
		return false
	}
	s.serveEnvelope(w, r, 404, "replay_not_found")
	return true
}

// hasQueryKey reports whether a parameter's name, percent-decoded
// leniently, is key.
func hasQueryKey(rawQuery, key string) bool {
	if rawQuery == "" {
		return false
	}
	for _, part := range strings.Split(rawQuery, "&") {
		name, _, _ := strings.Cut(part, "=")
		if string(s3.PercentDecode(name)) == key {
			return true
		}
	}
	return false
}

// serveEnvelope answers a request no route claims (404) or two routes
// claim (400) with the replay's own small JSON document.
func (s *Server) serveEnvelope(w http.ResponseWriter, r *http.Request, status int, reason string) {
	arrived := time.Now()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-s.stopCtx.Done():
	}
	header := ""
	if !s.begin() {
		s.reg.Get(metrics.ResponseAdmissionRefused, protocolReplay, "shutdown").Inc()
		status, reason, header = 503, "shutdown", "shutdown"
	} else {
		defer s.end()
	}
	var body []byte
	if r.Method != http.MethodHead {
		body = []byte(`{"error":"` + reason + `"}`)
	}
	s.writeJSON(w, r, protocolReplay, status, "application/json", body, header, false, arrived)
}

// serveGCS answers objects.list: the S3 route's admission, budget, delay
// and shutdown rules with the GCS request, pager, renderer and error
// envelope.
func (s *Server) serveGCS(w http.ResponseWriter, r *http.Request, path, rawQuery string) {
	arrived := time.Now()
	rt := s.routes
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-s.stopCtx.Done():
	}
	fail := func(kind gcs.Kind, reason string) {
		var body []byte
		if r.Method != http.MethodHead {
			body = gcs.AppendError(nil, kind)
		}
		s.writeJSON(w, r, ProtocolGCS, kind.Status(), gcs.ContentType, body, reason, kind == gcs.WrongMethod, arrived)
	}
	refuse := func(kind gcs.Kind, reason string) {
		s.reg.Get(metrics.ResponseAdmissionRefused, ProtocolGCS, reason).Inc()
		fail(kind, reason)
	}
	if !s.begin() {
		refuse(gcs.Overload, "shutdown")
		return
	}
	defer s.end()
	rt.gcsActive.Add(1)
	defer rt.gcsActive.Add(-1)
	countPath := func(p, reason string) { s.reg.Get(ProviderPath, ProtocolGCS, p, reason).Inc() }

	if r.Method != http.MethodGet {
		countPath("request_refused", "wrong_method")
		fail(gcs.WrongMethod, "wrong_method")
		return
	}
	if path != gcsPathPrefix+s.cfg.Bucket+"/o" {
		countPath("request_refused", "bucket_not_found")
		fail(gcs.NotFound, "bucket_not_found")
		return
	}
	req, err := gcs.Parse(s.cfg.Bucket, rawQuery)
	if err != nil {
		var f *gcs.Failure
		errors.As(err, &f)
		countPath("query_refused", f.Reason)
		fail(f.Kind(), f.Reason)
		return
	}
	if err := rt.gcs.ValidateToken(req); err != nil {
		countPath("token_refused", "invalid_or_mismatched")
		fail(gcs.Malformed, "invalid_page_token")
		return
	}
	if !s.tryResponse() {
		refuse(gcs.Overload, "response_count_exhausted")
		return
	}
	defer s.responses.Add(-1)
	reserved := int64(min(s.cfg.MaxResponseBytes, gcs.EstimateSize(req.PageSize)))
	if !s.budget.tryCharge(reserved) {
		refuse(gcs.Overload, "response_budget_exhausted")
		return
	}
	defer func() { s.budget.release(reserved) }()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(s.stopCtx, cancel)()

	started := time.Now()
	page, err := rt.gcs.List(ctx, req)
	if err != nil {
		var f *gcs.Failure
		switch {
		case s.stopCtx.Err() != nil:
			fail(gcs.Overload, "shutdown")
		case errors.As(err, &f):
			fail(f.Kind(), f.Reason)
		case store.Refusal(err) == "row_group_disorder":
			fmt.Fprintf(s.cfg.Log, "replay refused a request over a disordered fixture: %v\n", err)
			fail(gcs.FixtureDisordered, "fixture_disordered")
		default:
			fmt.Fprintf(s.cfg.Log, "replay failed to serve request: %v\n", err)
			fail(gcs.Internal, "internal")
		}
		return
	}
	cost := time.Since(started)
	s.reg.Get(metrics.RequestStageLatency, ProtocolGCS, "page").Record(cost)
	shape := latency.Classify(len(req.Delimiter) > 0, int64(req.PageSize))
	delay := s.cfg.Latency.Delay(shape, int64(len(page.Prefixes)), req.LatencyKey())

	renderStart := time.Now()
	body, err := gcs.AppendPage(make([]byte, 0, reserved), req, page)
	if err != nil {
		var p *gcs.FixtureProblem
		errors.As(err, &p)
		countPath("fixture_rejected", p.Reason)
		fail(gcs.FixtureIncompatible, p.Reason)
		return
	}
	s.reg.Get(metrics.RequestStageLatency, ProtocolGCS, "render").Record(time.Since(renderStart))
	countPath("timestamp_render", gcs.DaysRendered(page))
	if len(body) > s.cfg.MaxResponseBytes {
		refuse(gcs.ResponseTooLarge, "response_too_large")
		return
	}
	if extra := int64(len(body)) - reserved; extra > 0 {
		if !s.budget.tryCharge(extra) {
			refuse(gcs.Overload, "response_budget_exhausted")
			return
		}
		reserved += extra
	}
	if delay > 0 {
		if remaining := delay - time.Since(started); remaining > 0 {
			delayStart := time.Now()
			t := time.NewTimer(remaining)
			select {
			case <-t.C:
			case <-s.stopCtx.Done():
				t.Stop()
				fail(gcs.Overload, "shutdown")
				return
			}
			s.reg.Get(metrics.RequestStageLatency, ProtocolGCS, "delay").Record(time.Since(delayStart))
		} else {
			s.reg.Get(metrics.InjectOverrun, shape.String()).Inc()
			s.reg.Get(metrics.InjectOverrunMs, shape.String()).Observe(float64(-remaining) / 1e6)
		}
	}
	neutral := neutralShape(shape)
	s.reg.Get(metrics.ProtocolObjects, ProtocolGCS, neutral).Add(int64(len(page.Items)))
	s.reg.Get(metrics.ProtocolPrefixes, ProtocolGCS, neutral).Add(int64(len(page.Prefixes)))
	s.reg.Get(metrics.ProtocolEncodedBytes, ProtocolGCS, neutral).Add(int64(len(body)))
	s.writeJSON(w, r, ProtocolGCS, 200, gcs.ContentType, body, "", false, arrived)
}

// writeJSON sends an answer on a JSON route: Content-Type, Content-Length,
// the replay's reason header when there is one, Allow on a 405, and the
// body in views of at most OutputChunkBytes under the write deadline.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, protocol string, status int, contentType string, body []byte, reason string, allow bool, arrived time.Time) {
	h := w.Header()
	h["Content-Type"] = []string{contentType}
	h["Content-Length"] = []string{strconv.Itoa(len(body))}
	if reason != "" {
		h["x-swath-replay-error"] = []string{reason}
	}
	if allow {
		h["Allow"] = []string{"GET"}
	}
	s.reg.Get(metrics.HTTPRequests).Inc()
	if status >= 500 {
		s.reg.Get(metrics.HTTPErrors).Inc()
	}
	s.reg.Get(metrics.ProtocolHTTPRequests, protocol, strconv.Itoa(status/100)+"xx").Inc()
	writeStart := time.Now()
	_ = http.NewResponseController(w).SetWriteDeadline(writeStart.Add(s.cfg.WriteTimeout))
	w.WriteHeader(status)
	var err error
	if r.Method != http.MethodHead {
		for len(body) > 0 && err == nil {
			n := min(len(body), OutputChunkBytes)
			_, err = w.Write(body[:n])
			body = body[n:]
		}
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		s.reg.Get(metrics.ResponseWriteDeadline, protocol, "total_deadline").Inc()
	}
	if protocol != protocolReplay {
		s.reg.Get(metrics.RequestStageLatency, protocol, "write").Record(time.Since(writeStart))
	}
	s.reg.Get(metrics.HTTPRequestLatency).Record(time.Since(arrived))
}
