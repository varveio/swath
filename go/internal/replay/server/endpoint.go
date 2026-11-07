// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store"
)

// The metrics port is a second listener on purpose: a scrape never waits
// behind the request ceiling, never takes a store read, never sleeps in an
// injected delay and never moves a listing meter.

func (s *Server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.accepting.Load() {
		plain(w, http.StatusServiceUnavailable, "text/plain", "stopping\n")
		return
	}
	path, _ := requestTarget(r)
	switch path {
	case "/healthz":
		plain(w, http.StatusOK, "text/plain", "ok\n")
	case "/runtime-attestation":
		plain(w, http.StatusOK, "application/json", string(systemAttestation().render())+"\n")
	case "/metrics":
		plain(w, http.StatusOK, "application/json", string(s.Snapshot(time.Now()))+"\n")
	default:
		plain(w, http.StatusNotFound, "text/plain", "not found; this port serves /metrics, /runtime-attestation, and /healthz only\n")
	}
}

func plain(w http.ResponseWriter, status int, contentType, body string) {
	w.Header()["Content-Type"] = []string{contentType}
	w.Header()["Content-Length"] = []string{strconv.Itoa(len(body))}
	w.WriteHeader(status)
	w.Write([]byte(body))
}

// Snapshot is the schema-2 metrics payload: the serving configuration, the
// uptime (so two scrapes bound an interval on the server's clock) and
// every meter.
func (s *Server) Snapshot(now time.Time) []byte {
	c := &s.cfg
	out := []byte(`{"schema_version":2,"serving_mode":`)
	out = metrics.AppendString(out, c.ServingMode)
	out = append(out, `,"serving":{"protocols":`...)
	out = c.appendServingProtocols(out)
	out = append(out, `,"bucket":`...)
	out = metrics.AppendString(out, c.Bucket)
	out = append(out, `,"azure_account":null,"fixture_identity":`...)
	out = metrics.AppendString(out, c.FixtureIdentity)
	out = append(out, `,"ordering_profile":`...)
	out = metrics.AppendString(out, store.OrderingProfile)
	out = append(out, `,"metadata_policy":"fixture-plus-synthetic"`...)
	for _, f := range []struct {
		name string
		v    int64
	}{
		{"max_concurrent_requests", int64(c.MaxConcurrentRequests)},
		{"max_responses", int64(c.MaxResponses())},
		{"read_permit_limit", 0},
		{"response_buffer_budget", c.ResponseBufferBudget},
		{"max_response_bytes", int64(c.MaxResponseBytes)},
		{"output_chunk_bytes", OutputChunkBytes},
		{"charged_response_bytes", s.budget.charged.Load()},
		{"peak_charged_response_bytes", s.budget.peak.Load()},
		{"active_responses", s.responses.Load()},
		{"stop_timeout_ms", c.StopTimeout.Milliseconds()},
		{"idle_timeout_ms", c.IdleTimeout.Milliseconds()},
		{"write_timeout_ms", c.WriteTimeout.Milliseconds()},
	} {
		out = append(out, `,"`+f.name+`":`...)
		out = strconv.AppendInt(out, f.v, 10)
	}
	injection := "off"
	if c.Latency != nil {
		injection = "shape"
	}
	out = append(out, `,"latency_injection":"`+injection+`","pagination_profile":"default","profiles":`...)
	out = c.appendServingProfiles(out)
	out = append(out, '}')
	out = append(out, `,"uptime_ms":`...)
	out = strconv.AppendInt(out, now.Sub(s.started).Milliseconds(), 10)
	out = append(out, `,"sampled_at_epoch_ms":`...)
	out = strconv.AppendInt(out, now.UnixMilli(), 10)
	out = append(out, `,"meters":`...)
	out = s.reg.AppendMeters(out)
	return append(out, '}')
}

// MetricsEndpoint is the URL a startup line reports for the metrics port,
// with an IPv6 host bracketed.
func MetricsEndpoint(host string, port int) string {
	return "http://" + bracket(host) + ":" + strconv.Itoa(port) + "/metrics"
}

func bracket(host string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]"
	}
	return host
}
