// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"crypto/tls"
	"net/http"
)

// connWorkers is the workers per pooled transport (one HTTP/2 connection
// each, below the server's stream limit); maxConns caps the pool.
const connWorkers, maxConns = 16, 8

// Pool is the HTTP clients a listing client spreads its requests over,
// round-robin; each provider installs its own Guard and SDK client on
// every one. Built (supplied nil): one transport per connWorkers workers,
// at least one and at most maxConns, because Go's HTTP/2 transport opens a
// second connection only at the server's stream limit (~100), so one
// transport would carry every worker on one connection. Each is a clone
// of http.DefaultTransport keeping up to 1,024 idle connections per host,
// and all share one TLS session cache, so later connections resume.
// Supplied: a copy of it, alone, over its transport (http.DefaultTransport
// when nil). No client follows a redirect, a supplied one included: a
// redirect is an answer, since the resume identity freezes where requests
// go. wrap, if set, wraps each transport.
func Pool(workers int, supplied *http.Client, wrap func(http.RoundTripper) http.RoundTripper) []*http.Client {
	if wrap == nil {
		wrap = func(rt http.RoundTripper) http.RoundTripper { return rt }
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if supplied != nil {
		hc := *supplied
		hc.CheckRedirect = noRedirect
		if hc.Transport == nil {
			hc.Transport = http.DefaultTransport
		}
		hc.Transport = wrap(hc.Transport)
		return []*http.Client{&hc}
	}
	cache := tls.NewLRUClientSessionCache(0)
	pool := make([]*http.Client, min(max((workers+connWorkers-1)/connWorkers, 1), maxConns))
	for i := range pool {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns, t.MaxIdleConnsPerHost = 0, 1024
		t.TLSClientConfig = &tls.Config{ClientSessionCache: cache}
		pool[i] = &http.Client{Transport: wrap(t), CheckRedirect: noRedirect}
	}
	return pool
}
