// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolSize(t *testing.T) {
	for workers, want := range map[int]int{-1: 1, 0: 1, 1: 1, 16: 1, 17: 2, 64: 4, 128: 8, 129: 8, 1024: 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			pool := Pool(workers, nil, nil)
			if len(pool) != want {
				t.Fatalf("%d clients, want %d", len(pool), want)
			}
			seen := map[*http.Transport]bool{}
			cache := pool[0].Transport.(*http.Transport).TLSClientConfig.ClientSessionCache
			if cache == nil {
				t.Fatal("no TLS session cache")
			}
			for _, hc := range pool {
				tr := hc.Transport.(*http.Transport)
				t.Cleanup(tr.CloseIdleConnections)
				if seen[tr] || tr == http.DefaultTransport || tr.MaxIdleConns != 0 || tr.MaxIdleConnsPerHost != 1024 {
					t.Fatalf("transport reused or wrongly sized: %+v", tr)
				}
				seen[tr] = true
				if tr.TLSClientConfig.ClientSessionCache != cache {
					t.Error("TLS session caches are not shared")
				}
			}
		})
	}
}

func TestPoolPreservesDefaultTLS(t *testing.T) {
	original := http.DefaultTransport
	defer func() { http.DefaultTransport = original }()
	tr := original.(*http.Transport).Clone()
	cache := tls.NewLRUClientSessionCache(1)
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS13,
		ServerName: "custom.test", RootCAs: x509.NewCertPool(), NextProtos: []string{"h2"}, ClientSessionCache: cache}
	http.DefaultTransport = tr
	pool := Pool(64, nil, nil)
	for _, hc := range pool {
		got := hc.Transport.(*http.Transport).TLSClientConfig
		want := tr.TLSClientConfig.Clone()
		want.ClientSessionCache = got.ClientSessionCache
		if got == tr.TLSClientConfig || !reflect.DeepEqual(got, want) || got.ClientSessionCache == cache {
			t.Fatalf("TLS settings lost or cache reused: %+v", got)
		}
	}
	if tr.TLSClientConfig.ClientSessionCache != cache {
		t.Fatal("default transport's TLS cache changed")
	}
}

func TestPoolSuppliedClient(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, rt := range []http.RoundTripper{nil, &http.Transport{TLSClientConfig: &tls.Config{ServerName: "supplied.test"}}} {
		t.Run(fmt.Sprintf("%T", rt), func(t *testing.T) {
			redirectErr := fmt.Errorf("supplied redirect policy")
			supplied := &http.Client{Transport: rt, Timeout: 3 * time.Second, Jar: jar,
				CheckRedirect: func(*http.Request, []*http.Request) error { return redirectErr }}
			var wrapped http.RoundTripper
			var calls int
			pool := Pool(128, supplied, func(next http.RoundTripper) http.RoundTripper {
				wrapped = next
				calls++
				return &Guard{Next: next}
			})
			if len(pool) != 1 || pool[0] == supplied || calls != 1 {
				t.Fatalf("pool %v, wrap calls %d", pool, calls)
			}
			hc := pool[0]
			wantRT := rt
			if wantRT == nil {
				wantRT = http.DefaultTransport
			}
			if wrapped != wantRT || hc.Transport.(*Guard).Next != wantRT || hc.Timeout != supplied.Timeout || hc.Jar != jar {
				t.Fatalf("supplied settings lost: %+v", hc)
			}
			if supplied.Transport != rt || supplied.CheckRedirect(nil, nil) != redirectErr || hc.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
				t.Fatal("supplied client mutated or redirects allowed")
			}
			if tr, ok := rt.(*http.Transport); ok && (tr.TLSClientConfig.ServerName != "supplied.test" || tr.TLSClientConfig.ClientSessionCache != nil) {
				t.Fatal("supplied TLS settings changed")
			}
		})
	}
}

func TestPoolRedirectPolicy(t *testing.T) {
	var followed atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Add(1) }))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	for _, supplied := range []*http.Client{nil, {}} {
		hc := Pool(1, supplied, nil)[0]
		resp, err := hc.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusTemporaryRedirect || followed.Load() != 0 {
			t.Fatalf("status %d, %d redirects followed", resp.StatusCode, followed.Load())
		}
		hc.CloseIdleConnections()
	}
}

// Independent transports open independent HTTP/2 connections below the
// server's stream limit; each carries its share of a concurrent burst.
func TestPoolSpreadsConnections(t *testing.T) {
	for _, workers := range []int{1, 16, 64, 128} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			var mu sync.Mutex
			perConn := map[string]int{}
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ProtoMajor != 2 {
					t.Errorf("request over %s, want HTTP/2", r.Proto)
				}
				mu.Lock()
				perConn[r.RemoteAddr]++
				mu.Unlock()
				fmt.Fprint(w, "ok")
			}))
			srv.EnableHTTP2 = true
			srv.StartTLS()
			defer srv.Close()
			pool := Pool(workers, nil, nil)
			roots := srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
			for _, hc := range pool {
				hc.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
				t.Cleanup(hc.CloseIdleConnections)
			}
			get := func(hc *http.Client) {
				resp, err := hc.Get(srv.URL)
				if err != nil {
					t.Error(err)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			for _, hc := range pool {
				get(hc)
			}
			const per = 8
			var wg sync.WaitGroup
			for i := range len(pool) * per {
				wg.Go(func() { get(pool[i%len(pool)]) })
			}
			wg.Wait()
			if len(perConn) != len(pool) {
				t.Fatalf("%d connections %v, want %d", len(perConn), perConn, len(pool))
			}
			for addr, k := range perConn {
				if k != per+1 {
					t.Errorf("connection %s carried %d requests, want %d", addr, k, per+1)
				}
			}
		})
	}
}
