// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package server is the replay HTTP server: the S3 ListObjectsV2 route over
// a store.Store, request admission, injected latency, graceful shutdown,
// and the metrics port (/metrics, /healthz, /runtime-attestation).
//
// Routing is a plain http.Handler rather than the pattern ServeMux: with
// only the S3 route enabled every path belongs to it (the bucket is the
// first segment of the path as sent), and ServeMux would redirect
// unclean paths such as //b or /a/../b before the route could answer
// them as S3 does.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/replay/latency"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
)

// Defaults and bounds of the serving configuration.
const (
	DefaultMaxConcurrentRequests = 512
	// MinMaxConcurrentRequests keeps a few requests servable while others
	// sit in long injected delays.
	MinMaxConcurrentRequests    = 8
	DefaultResponseBufferBudget = 256 << 20
	DefaultMaxResponseBytes     = 64 << 20
	DefaultStopTimeout          = 10 * time.Second
	DefaultIdleTimeout          = 30 * time.Second
	DefaultWriteTimeout         = 30 * time.Second
	// OutputChunkBytes is reported in the startup line and the metrics
	// payload; responses are written in views of at most this size.
	OutputChunkBytes = 256 << 10
)

// Config is a validated serving configuration.
type Config struct {
	Bucket string
	Host   string
	Port   int
	// MetricsPort serves the metrics endpoint; negative disables it, 0
	// binds a free port.
	MetricsPort           int
	MaxConcurrentRequests int
	ResponseBufferBudget  int64
	MaxResponseBytes      int
	StopTimeout           time.Duration
	IdleTimeout           time.Duration
	WriteTimeout          time.Duration
	// Latency is the injected profile; nil injects nothing.
	Latency *latency.Profile
	// SlowRequestLog logs requests whose own cost reaches it; negative
	// disables.
	SlowRequestLog time.Duration
	Store          store.Store
	// ServingMode names the backing store in the metrics payload and the
	// serving.path meter (SORTED for a sorted fixture).
	ServingMode     string
	FixtureIdentity string
	// IndexLoad is how long opening the fixture took (footers read and the
	// routing index built) and IndexEntries the routing entries it built,
	// for index.load.latency and index.entries; a zero IndexLoad records
	// neither.
	IndexLoad    time.Duration
	IndexEntries int
	// Log receives the slow-request and shutdown lines.
	Log io.Writer
	// Protocols are the enabled listing routes (ProtocolS3, ProtocolGCS,
	// ProtocolAzure); empty is S3 only.
	Protocols []string
	// AzureAccount is the Azure route's account path segment and
	// AdvertisedHost the host its ServiceEndpoint names (Host when empty).
	AzureAccount, AdvertisedHost string
}

// Validate checks the bounds the server relies on (not the store, which is
// opened after the flags are checked).
func (c *Config) Validate() error {
	switch {
	case c.MaxConcurrentRequests < MinMaxConcurrentRequests:
		return fmt.Errorf("max concurrent requests must be at least %d", MinMaxConcurrentRequests)
	case c.MaxConcurrentRequests > 1<<30:
		return errors.New("max concurrent requests exceeds response-count range")
	case c.ResponseBufferBudget <= 0 || c.MaxResponseBytes <= 0 || int64(c.MaxResponseBytes) > c.ResponseBufferBudget:
		return errors.New("invalid response buffer bounds")
	case c.StopTimeout <= 0 || c.IdleTimeout <= 0 || c.WriteTimeout <= 0:
		return errors.New("serve timeouts must be positive")
	}
	return c.validateAzure()
}

// MaxResponses is the response-count ceiling: twice the request ceiling.
func (c *Config) MaxResponses() int { return 2 * c.MaxConcurrentRequests }

// Server serves one store.
type Server struct {
	cfg   Config
	reg   *metrics.Registry
	pager *s3.Pager

	slots     chan struct{}
	responses atomic.Int64
	budget    *output.Budget

	stopCtx  context.Context
	stop     context.CancelFunc
	started  time.Time
	shutOnce sync.Once

	// opMu guards the count of requests that hold the store and the
	// closing flag together, so no request can begin once shutdown has
	// started waiting for the count to reach zero.
	opMu    sync.Mutex
	closing bool
	active  int
	drained chan struct{} // closed when active reaches zero after closing

	httpSrv, metricsSrv *http.Server
	ln, metricsLn       net.Listener
	accepting           atomic.Bool
	metricsConns        atomic.Int64   // open metrics connections
	conns               *limitListener // the serving listener, bounded
	s3Active            atomic.Int64
	routes              *routes // nil: the S3 route answers every request
	azure               *azureRoute
}

// New returns a server for cfg; Listen binds it.
func New(cfg Config) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Store == nil {
		return nil, errors.New("no store")
	}
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	s := &Server{
		cfg:     cfg,
		reg:     metrics.NewRegistry(),
		slots:   make(chan struct{}, cfg.MaxConcurrentRequests),
		started: time.Now(),
	}
	s.stopCtx, s.stop = context.WithCancel(context.Background())
	s.budget = output.NewBudget(cfg.ResponseBufferBudget, func(r output.Reason, n int) {
		s.reg.Get(metrics.ResponseChunkAllocation, r.String()).Inc()
		s.reg.Get(metrics.ResponseChunkCapacityBytes, r.String()).Observe(float64(n))
	})
	s.initRoutes()
	s.pager = &s3.Pager{Store: cfg.Store, OnDelimiterPath: func(p string) { s.reg.Get(metrics.DelimiterPath, p).Inc() }}
	// The meters every scrape shows from the start, so a reader can tell a
	// zero from a meter that is not there.
	for _, d := range []metrics.Def{metrics.HTTPRequests, metrics.HTTPErrors, metrics.HTTPRequestLatency, metrics.FixtureListLatency} {
		s.reg.Get(d)
	}
	for _, sh := range latency.Shapes {
		s.reg.Get(metrics.RequestLatency, sh.String())
	}
	s.reg.Get(metrics.ServingPath, servingPathTag(cfg.ServingMode))
	for _, r := range output.Reasons {
		s.reg.Get(metrics.ResponseChunkAllocation, r.String())
		s.reg.Get(metrics.ResponseChunkCapacityBytes, r.String())
	}
	s.reg.SetGauge(metrics.ResponseBytesLive, func() float64 { return float64(s.budget.Charged()) })
	s.reg.SetGauge(metrics.ResponseBytesPeak, func() float64 { return float64(s.budget.Peak()) })
	s.reg.SetGauge(metrics.ResponseActive, func() float64 { return float64(s.responses.Load()) })
	s.reg.SetGauge(metrics.ProtocolRequestsActive, func() float64 { return float64(s.s3Active.Load()) }, "s3")
	s.initStoreMeters()
	s.newAzureRoute()
	s.httpSrv = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: cfg.IdleTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.maxHeaderBytes(),
		ErrorLog:          nil,
	}
	return s, nil
}

func servingPathTag(mode string) string {
	b := []byte(mode)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// Registry is the server's meter registry.
func (s *Server) Registry() *metrics.Registry { return s.reg }

// Listen binds the serving port and, when enabled, the metrics port.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port)))
	if err != nil {
		return err
	}
	s.conns = &limitListener{Listener: ln, max: int64(s.cfg.MaxConnections())}
	s.ln = s.conns
	if s.cfg.MetricsPort >= 0 {
		mln, err := net.Listen("tcp", net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.MetricsPort)))
		if err != nil {
			ln.Close()
			return fmt.Errorf("metrics port: %w", err)
		}
		s.metricsLn = mln
		// The metrics listener bypasses listing admission, so its
		// connections are bounded by time instead: the serving
		// listener's --idle-timeout for an idle keep-alive connection, for
		// reading a request and for its body, and --write-timeout for
		// writing an answer to a client that stopped reading
		// (TestMetricsConnectionsTimeOut).
		s.metricsSrv = &http.Server{
			Handler:           http.HandlerFunc(s.serveMetrics),
			ReadHeaderTimeout: s.cfg.IdleTimeout,
			ReadTimeout:       s.cfg.IdleTimeout,
			WriteTimeout:      s.cfg.WriteTimeout,
			IdleTimeout:       s.cfg.IdleTimeout,
			ConnState: func(_ net.Conn, st http.ConnState) {
				switch st {
				case http.StateNew:
					s.metricsConns.Add(1)
				case http.StateClosed, http.StateHijacked:
					s.metricsConns.Add(-1)
				}
			},
		}
	}
	return nil
}

// Port is the bound serving port.
func (s *Server) Port() int { return s.ln.Addr().(*net.TCPAddr).Port }

// MetricsPort is the bound metrics port, or -1 when it is off.
func (s *Server) MetricsPort() int {
	if s.metricsLn == nil {
		return -1
	}
	return s.metricsLn.Addr().(*net.TCPAddr).Port
}

// Serve serves both ports until Shutdown. The metrics port answers
// /healthz once the serving port is accepting.
func (s *Server) Serve() error {
	errc := make(chan error, 2)
	go func() { errc <- s.httpSrv.Serve(s.ln) }()
	if s.metricsSrv != nil {
		s.accepting.Store(true)
		go func() { errc <- s.metricsSrv.Serve(s.metricsLn) }()
	}
	err := <-errc
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops the server within timeout: new requests are refused,
// requests waiting for admission or sleeping in an injected delay are
// answered 503 at once, and the store is closed once every request that
// holds it has returned. Everything, the store's Close included, is bounded
// by the one deadline: past it Shutdown logs shutdown_incomplete, reports
// false and leaves the remaining cleanup to finish in the background (a
// store read that ignores cancellation keeps the store open until it
// returns). A store Close failure is logged and returned once. A second
// call does nothing and reports true, nil.
func (s *Server) Shutdown(timeout time.Duration) (bool, error) {
	complete, failed := true, error(nil)
	s.shutOnce.Do(func() {
		deadline := time.Now().Add(timeout)
		s.opMu.Lock()
		s.closing = true
		s.drained = make(chan struct{})
		if s.active == 0 {
			close(s.drained)
		}
		drained := s.drained
		s.opMu.Unlock()
		s.accepting.Store(false)
		s.stop()
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		var wg sync.WaitGroup
		for _, srv := range []*http.Server{s.httpSrv, s.metricsSrv} {
			if srv == nil {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := srv.Shutdown(ctx); err != nil {
					srv.Close()
				}
			}()
		}
		wg.Wait()
		if ctx.Err() != nil {
			fmt.Fprintln(s.cfg.Log, "shutdown_incomplete HTTP server stop exceeded its deadline")
			complete = false
		}
		// The store closes after the last request that holds it, on its
		// own goroutine, so a Close that blocks cannot outlast the
		// deadline.
		cleaned := make(chan error, 1)
		go func() {
			<-drained
			err := s.cfg.Store.Close()
			if err != nil {
				fmt.Fprintf(s.cfg.Log, "replay store cleanup failed: %v\n", err)
			}
			cleaned <- err
		}()
		select {
		case err := <-cleaned:
			if err != nil {
				failed = fmt.Errorf("failed to close S3 listing replay fixture: %w", err)
			}
		case <-ctx.Done():
			fmt.Fprintln(s.cfg.Log, "shutdown_incomplete active replay work retained its resources for deferred cleanup")
			complete = false
		}
	})
	return complete, failed
}

// begin registers a request that will use the store; false once shutdown
// has begun.
func (s *Server) begin() bool {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closing {
		return false
	}
	s.active++
	return true
}

func (s *Server) end() {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.active--
	if s.active == 0 && s.closing {
		close(s.drained)
	}
}

// tryResponse takes one of the MaxResponses response permits.
func (s *Server) tryResponse() bool {
	for {
		cur := s.responses.Load()
		if cur >= int64(s.cfg.MaxResponses()) {
			return false
		}
		if s.responses.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}
