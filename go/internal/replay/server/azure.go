// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/replay/azure"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
)

// The Azure Blob flat List Blobs route, /{account}/{container}?restype=
// container&comp=list, beside the S3 route. Enabled native selectors claim
// their requests before the method is checked (a POST with the Azure
// selector is the Azure route's UnsupportedHttpVerb, not an S3 405), a
// request carrying both the Azure and the S3 selector is a replay 400, and
// with S3 disabled every unclaimed request is a replay 404: the replay wire
// contract's routing (TestAzureRouting).

const (
	protocolAzure  = "azure"
	protocolReplay = "replay"
	// DefaultAzureAccount is the account path segment without
	// --azure-account.
	DefaultAzureAccount = "replay"
	azureOrdering       = "azure=utf16-code-unit-order"
)

// ProviderPath counts engaged provider paging paths and the classification
// that selected each.
var ProviderPath = metrics.Def{Name: "swath.replay.provider.path", Kind: metrics.Counter, Tags: []string{"protocol", "path", "reason"}}

// EnabledProtocols is the sorted list of enabled routes.
func (c *Config) EnabledProtocols() []string {
	if len(c.Protocols) == 0 {
		return []string{protocolS3}
	}
	p := slices.Clone(c.Protocols)
	slices.Sort(p)
	return slices.Compact(p)
}

func (c *Config) enabled(protocol string) bool {
	return slices.Contains(c.EnabledProtocols(), protocol)
}

// OrderingProfile names the key order: the fixture's byte order, and the
// Azure route's UTF-16 code-unit order when that route is enabled.
func (c *Config) OrderingProfile() string {
	if c.enabled(protocolAzure) {
		return store.OrderingProfile + ";" + azureOrdering
	}
	return store.OrderingProfile
}

// ProtocolProfile is the wire profile a route serves.
func ProtocolProfile(protocol string) string {
	switch protocol {
	case protocolS3:
		return "list-objects-v2"
	case protocolAzure:
		return "blob-list-flat-" + azure.Version20260606 + "," + azure.Version20261006
	case "gcs":
		return "json-v1"
	}
	return ""
}

var (
	azureAccountName   = regexp.MustCompile(`^[a-z0-9]{3,24}$`)
	azureContainerName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
)

// validateAzure checks the Azure route's settings: an account of 3 to 24
// lower-case letters and digits, a bucket that is a valid container name,
// and an advertised host for a wildcard bind (the ServiceEndpoint must
// name a reachable host).
func (c *Config) validateAzure() error {
	for _, p := range c.Protocols {
		if p != protocolS3 && p != protocolAzure {
			return fmt.Errorf("unknown protocol %q", p)
		}
	}
	if !c.enabled(protocolAzure) {
		if c.AzureAccount != "" {
			return errors.New("--azure-account requires azure in --protocols")
		}
		return nil
	}
	if !azureAccountName.MatchString(c.AzureAccount) {
		return errors.New("invalid Azure account name")
	}
	if !azureContainerName.MatchString(c.Bucket) || strings.Contains(c.Bucket, "--") {
		return errors.New("bucket cannot be exposed as an Azure container")
	}
	if wildcardHost(c.Host) && strings.TrimSpace(c.AdvertisedHost) == "" {
		return errors.New("--advertised-host is required for wildcard Azure binds")
	}
	return nil
}

func wildcardHost(host string) bool {
	if host == "0.0.0.0" {
		return true
	}
	if !strings.Contains(host, ":") {
		return false
	}
	ip := net.ParseIP(strings.TrimSuffix(strings.TrimPrefix(host, "["), "]"))
	return ip != nil && ip.IsUnspecified()
}

// azureRoute is the Azure route's state.
type azureRoute struct {
	pager    *azure.Pager
	active   atomic.Int64
	requests atomic.Uint64 // x-ms-request-id sequence
}

func (s *Server) newAzureRoute() {
	if !s.cfg.enabled(protocolAzure) {
		return
	}
	a := &azureRoute{}
	a.pager = &azure.Pager{Store: s.cfg.Store, FixtureIdentity: s.cfg.FixtureIdentity, OnPath: s.azurePath}
	s.azure = a
	s.reg.SetGauge(metrics.ProtocolRequestsActive, func() float64 { return float64(a.active.Load()) }, protocolAzure)
}

func (s *Server) azurePath(path, reason string) {
	s.reg.Get(ProviderPath, protocolAzure, path, reason).Inc()
}

// azureEndpoint is the ServiceEndpoint: http://host:port/account/.
func (s *Server) azureEndpoint() string {
	host := s.cfg.AdvertisedHost
	if host == "" {
		host = s.cfg.Host
	}
	return "http://" + bracket(host) + ":" + strconv.Itoa(s.Port()) + "/" + s.cfg.AzureAccount + "/"
}

// routeNative answers a request a native route claims, or one no enabled
// route claims; false leaves it to the S3 route.
func (s *Server) routeNative(w http.ResponseWriter, r *http.Request, path, rawQuery string, arrived time.Time) bool {
	if s.azure == nil {
		return false
	}
	query := rawQuery
	claimed := s.azureClaims(path) && (hasQueryKey(query, "restype") || hasQueryKey(query, "comp"))
	switch {
	case claimed && hasQueryKey(query, "list-type"):
		s.serveEnvelope(w, r, arrived, 400, "replay_ambiguous_request")
	case claimed:
		s.serveAzure(w, r, path, rawQuery, arrived)
	case s.cfg.enabled(protocolS3):
		return false
	default:
		s.serveEnvelope(w, r, arrived, 404, "replay_not_found")
	}
	return true
}

// azureClaims reports whether path is /{account}/{one segment}.
func (s *Server) azureClaims(path string) bool {
	container, ok := strings.CutPrefix(path, "/"+s.cfg.AzureAccount+"/")
	return ok && container != "" && !strings.Contains(container, "/")
}

// hasQueryKey reports whether a query parameter's percent-decoded name is
// key.
func hasQueryKey(query, key string) bool {
	if query == "" {
		return false
	}
	for _, field := range strings.Split(query, "&") {
		name, _, _ := strings.Cut(field, "=")
		if string(s3.PercentDecode(name)) == key {
			return true
		}
	}
	return false
}

// admit takes a request slot (queueing as a full worker pool would) and
// registers the request with the store; release undoes both. ok=false
// means shutdown has begun.
func (s *Server) admit() (release func(), ok bool) {
	select {
	case s.slots <- struct{}{}:
	case <-s.stopCtx.Done():
		return func() {}, false
	}
	if !s.begin() {
		return func() { <-s.slots }, false
	}
	return func() { s.end(); <-s.slots }, true
}

// serveEnvelope answers a request no route serves with the replay's own
// small JSON envelope.
func (s *Server) serveEnvelope(w http.ResponseWriter, r *http.Request, arrived time.Time, status int, reason string) {
	release, ok := s.admit()
	defer release()
	if !ok {
		s.reg.Get(metrics.ResponseAdmissionRefused, protocolReplay, "shutdown").Inc()
		status, reason = 503, "shutdown"
		w.Header()["x-swath-replay-error"] = []string{reason}
	}
	body := []byte(`{"error":"` + reason + `"}`)
	if r.Method == http.MethodHead {
		body = nil
	}
	h := w.Header()
	h["Content-Type"] = []string{"application/json"}
	h["Content-Length"] = []string{strconv.Itoa(len(body))}
	s.countResponse(protocolReplay, status, arrived)
	w.WriteHeader(status)
	w.Write(body)
}

func (s *Server) countResponse(protocol string, status int, arrived time.Time) {
	s.reg.Get(metrics.HTTPRequests).Inc()
	if status >= 500 {
		s.reg.Get(metrics.HTTPErrors).Inc()
	}
	s.reg.Get(metrics.ProtocolHTTPRequests, protocol, strconv.Itoa(status/100)+"xx").Inc()
	s.reg.Get(metrics.HTTPRequestLatency).Record(time.Since(arrived))
}

// The replay's own refusals on the Azure route.
var (
	azureStopping    = &azure.Error{Kind: azure.Overload, Reason: "shutdown"}
	azureCount       = &azure.Error{Kind: azure.Overload, Reason: "response_count_exhausted"}
	azureBudget      = &azure.Error{Kind: azure.Overload, Reason: "response_budget_exhausted"}
	azureTooLarge    = &azure.Error{Kind: azure.ResponseTooLarge, Reason: "response_too_large"}
	azureInternal    = &azure.Error{Kind: azure.Internal, Reason: "internal"}
	azureDisordered  = &azure.Error{Kind: azure.FixtureDisordered, Reason: "fixture_disordered"}
	azureInterrupted = azureStopping
)

func (s *Server) serveAzure(w http.ResponseWriter, r *http.Request, path, rawQuery string, arrived time.Time) {
	a := s.azure
	release, ok := s.admit()
	defer release()
	if !ok {
		s.azureRefuse(w, r, arrived, azureStopping)
		return
	}
	a.active.Add(1)
	defer a.active.Add(-1)

	req, err := azure.Parse(r.Method, path, rawQuery, r.Header, s.cfg.AzureAccount, s.cfg.Bucket)
	if err != nil {
		s.azurePath("request_refused", err.(*azure.Error).Reason)
		s.azureFail(w, r, arrived, err)
		return
	}
	if err := a.pager.ValidateMarker(req); err != nil {
		s.azureFail(w, r, arrived, err)
		return
	}
	if !s.tryResponse() {
		s.azureRefuse(w, r, arrived, azureCount)
		return
	}
	defer s.responses.Add(-1)
	reserved := int64(min(s.cfg.MaxResponseBytes, azure.EstimateSize(req.PageSize)))
	if !s.budget.tryCharge(reserved) {
		s.azureRefuse(w, r, arrived, azureBudget)
		return
	}
	defer func() { s.budget.release(reserved) }()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(s.stopCtx, cancel)()

	started := time.Now()
	res, err := a.pager.List(ctx, req)
	if err != nil {
		switch {
		case s.stopCtx.Err() != nil:
			err = azureInterrupted
		case store.Refusal(err) == "row_group_disorder":
			fmt.Fprintf(s.cfg.Log, "replay refused a request over a disordered fixture: %v\n", err)
			err = azureDisordered
		}
		s.azureFail(w, r, arrived, err)
		return
	}
	s.reg.Get(metrics.RequestStageLatency, protocolAzure, "page").Record(time.Since(started))
	shape := req.Shape()
	delay := s.cfg.Latency.Delay(shape, int64(res.Prefixes), req.LatencyKey())

	renderStart := time.Now()
	body, err := azure.AppendList(make([]byte, 0, reserved), res, s.azureEndpoint())
	if err != nil {
		s.azurePath("fixture_rejected", err.(*azure.Error).Reason)
		s.azureFail(w, r, arrived, err)
		return
	}
	s.reg.Get(metrics.RequestStageLatency, protocolAzure, "render").Record(time.Since(renderStart))
	if len(body) > s.cfg.MaxResponseBytes {
		s.azureRefuse(w, r, arrived, azureTooLarge)
		return
	}
	if extra := int64(len(body)) - reserved; extra > 0 {
		if !s.budget.tryCharge(extra) {
			s.azureRefuse(w, r, arrived, azureBudget)
			return
		}
		reserved += extra
	}
	// The injected delay is the request's total target time.
	if delay > 0 {
		if remaining := delay - time.Since(started); remaining > 0 {
			delayStart := time.Now()
			t := time.NewTimer(remaining)
			select {
			case <-t.C:
			case <-s.stopCtx.Done():
				t.Stop()
				s.azureFail(w, r, arrived, azureInterrupted)
				return
			}
			s.reg.Get(metrics.RequestStageLatency, protocolAzure, "delay").Record(time.Since(delayStart))
		} else {
			s.reg.Get(metrics.InjectOverrun, shape.String()).Inc()
			s.reg.Get(metrics.InjectOverrunMs, shape.String()).Observe(float64(-remaining) / 1e6)
		}
	}
	neutral := neutralShape(shape)
	s.reg.Get(metrics.ProtocolObjects, protocolAzure, neutral).Add(int64(len(res.Entries) - res.Prefixes))
	s.reg.Get(metrics.ProtocolPrefixes, protocolAzure, neutral).Add(int64(res.Prefixes))
	s.reg.Get(metrics.ProtocolEncodedBytes, protocolAzure, neutral).Add(int64(len(body)))
	s.azureWrite(w, r, 200, body, req, nil, arrived)
}

// azureFail answers an error: an Azure refusal as it is, anything else (a
// store failure) as a 500 that carries no detail.
func (s *Server) azureFail(w http.ResponseWriter, r *http.Request, arrived time.Time, err error) {
	var e *azure.Error
	if !errors.As(err, &e) {
		fmt.Fprintf(s.cfg.Log, "replay failed to serve request: %v\n", err)
		e = azureInternal
	}
	s.azureWriteError(w, r, arrived, e)
}

// azureRefuse answers an admission refusal and counts it.
func (s *Server) azureRefuse(w http.ResponseWriter, r *http.Request, arrived time.Time, e *azure.Error) {
	s.reg.Get(metrics.ResponseAdmissionRefused, protocolAzure, e.Reason).Inc()
	s.azureWriteError(w, r, arrived, e)
}

// azureWriteError sends the error envelope. The service version and the
// client request id are echoed from the request's headers when each was
// sent once and is valid, whatever else the request got wrong; a HEAD
// answer carries the headers and no body.
func (s *Server) azureWriteError(w http.ResponseWriter, r *http.Request, arrived time.Time, e *azure.Error) {
	var body []byte
	if r.Method != http.MethodHead {
		body = azure.AppendError(nil, e.Code())
	}
	echo := &azure.Request{}
	if vs := r.Header.Values("x-ms-version"); len(vs) == 1 {
		echo.Version = vs[0]
	}
	if vs := r.Header.Values("x-ms-client-request-id"); len(vs) == 1 {
		echo.ClientRequestID, echo.HasClientRequestID = vs[0], true
	}
	s.azureWrite(w, r, e.Status(), body, echo, e, arrived)
}

// azureWrite sends a response with the Azure route's headers.
func (s *Server) azureWrite(w http.ResponseWriter, r *http.Request, status int, body []byte, req *azure.Request, e *azure.Error, arrived time.Time) {
	h := w.Header()
	h["Content-Type"] = []string{"application/xml"}
	h["Content-Length"] = []string{strconv.Itoa(len(body))}
	h["x-ms-request-id"] = []string{"replay-" + strconv.FormatUint(s.azure.requests.Add(1), 16)}
	h["Date"] = []string{time.Now().UTC().Format(azure.HTTPTime)}
	if req.Version == azure.Version20260606 || req.Version == azure.Version20261006 {
		h["x-ms-version"] = []string{req.Version}
	}
	if req.HasClientRequestID && len(req.ClientRequestID) <= 1024 && !strings.ContainsAny(req.ClientRequestID, "\r\n") {
		h["x-ms-client-request-id"] = []string{req.ClientRequestID}
	}
	if e != nil {
		h["x-ms-error-code"] = []string{e.Code()}
		if strings.TrimSpace(e.Reason) != "" {
			h["x-swath-replay-error"] = []string{e.Reason}
		}
		if e.Kind == azure.WrongMethod {
			h["allow"] = []string{"GET"}
		}
	}
	s.countResponse(protocolAzure, status, arrived)
	writeStart := time.Now()
	_ = http.NewResponseController(w).SetWriteDeadline(writeStart.Add(s.cfg.WriteTimeout))
	w.WriteHeader(status)
	var err error
	for len(body) > 0 && err == nil {
		n := min(len(body), OutputChunkBytes)
		_, err = w.Write(body[:n])
		body = body[n:]
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		s.reg.Get(metrics.ResponseWriteDeadline, protocolAzure, "total_deadline").Inc()
	}
	s.reg.Get(metrics.RequestStageLatency, protocolAzure, "write").Record(time.Since(writeStart))
}
