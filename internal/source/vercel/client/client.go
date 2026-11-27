// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client lists Vercel Blob stores through the versioned Blob API.
package client

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
)

const (
	// Endpoint is the sole origin the client sends Blob credentials to.
	Endpoint = "https://vercel.com/api/blob"
	// Version pins the list format used by Vercel's Blob SDK.
	Version = "13"
	// MaxPageSize is the maximum requested number of blobs and folders per page.
	MaxPageSize = 1000
	// MaxBodyBytes caps response body bytes before JSON decoding. The
	// HTTP buffer and decoded data can allocate additional memory.
	MaxBodyBytes = 16 << 20
)

type Config struct {
	objhttp.Retry
	Token          string        // validated by the provider opener
	RequestTimeout time.Duration // per attempt; default 30 s
	Trust          source.Trust
	Workers        int
	HTTPClient     *http.Client // transport injection for tests
}

type Client struct {
	*objhttp.Meter
	http    []*http.Client
	token   string
	timeout time.Duration
	next    atomic.Uint64
}

func New(cfg Config) (*Client, error) {
	trust, err := cfg.Trust.Config()
	if err != nil {
		return nil, err
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = source.DefaultRequestTimeout
	}
	c := &Client{Meter: objhttp.NewMeter(cfg.Retry), token: cfg.Token, timeout: cfg.RequestTimeout}
	c.http = objhttp.Pool(cfg.Workers, cfg.HTTPClient, trust, func(rt http.RoundTripper) http.RoundTripper {
		return &objhttp.Guard{Next: listTransport{rt}, MaxBody: MaxBodyBytes}
	})
	return c, nil
}

// listTransport strips credential-bearing transport diagnostics before the
// Guard's request watch sees them. Redirect locations are dropped before
// net/http parses them; the status is enough to report a refusal.
type listTransport struct{ http.RoundTripper }

func (t listTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(r)
	if err != nil {
		if safe, redacted := redactWireError(err, r); redacted {
			err = &objhttp.GuardError{Err: safe}
		}
	}
	if resp != nil {
		if resp.StatusCode/100 == 3 {
			resp.Header.Del("Location")
		}
		resp.Body = &listBody{ReadCloser: resp.Body, request: r}
	}
	return resp, err
}

type listBody struct {
	io.ReadCloser
	request *http.Request
}

func (b *listBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		err, _ = redactWireError(err, b.request)
	}
	return n, err
}

func redactWireError(err error, req *http.Request) (error, bool) {
	cursor := req.URL.Query().Get("cursor")
	token, _ := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
	if !containsCredential(err.Error(), token, cursor) {
		return err, false
	}
	if objhttp.Transport(err) {
		var ne net.Error
		timedOut := errors.As(err, &ne) && ne.Timeout()
		return &redactedTransportError{timedOut: timedOut}, true
	}
	return errors.New("Vercel Blob transport error (details redacted)"), true
}

func containsCredential(message, token, cursor string) bool {
	return strings.Contains(message, "vercel_blob_") ||
		token != "" && strings.Contains(message, token) ||
		cursor != "" && (strings.Contains(message, cursor) || strings.Contains(message, url.QueryEscape(cursor)))
}

// redactedTransportError retains the retry and timeout decision when a
// network error's text contains a credential. It has no unsanitized cause.
type redactedTransportError struct{ timedOut bool }

func (e *redactedTransportError) Error() string {
	return "Vercel Blob transport error (details redacted)"
}
func (e *redactedTransportError) Timeout() bool   { return e.timedOut }
func (e *redactedTransportError) Temporary() bool { return true }

func (t listTransport) CloseIdleConnections() {
	if c, ok := t.RoundTripper.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

func (c *Client) PoolSize() int { return len(c.http) }

func (c *Client) CloseIdleConnections() {
	for _, h := range c.http {
		h.CloseIdleConnections()
	}
}

type ListInput struct {
	Prefix string
	Cursor string
	Limit  int // validated by the provider opener
}

// List fetches one folded listing page, retrying the same request when
// necessary. Its cursor is opaque and belongs to this prefix.
func (c *Client) List(ctx context.Context, in ListInput) (ListOutput, error) {
	q := url.Values{"mode": {"folded"}, "limit": {strconv.Itoa(in.Limit)}}
	if in.Prefix != "" {
		q.Set("prefix", in.Prefix)
	}
	if in.Cursor != "" {
		q.Set("cursor", in.Cursor)
	}
	target := Endpoint + "?" + q.Encode()
	return objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (ListOutput, error) {
		return c.listOnce(ctx, target, in.Limit)
	})
}

func (c *Client) listOnce(ctx context.Context, target string, limit int) (ListOutput, error) {
	actx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var capture objhttp.Capture
	req, err := http.NewRequestWithContext(objhttp.WithCapture(actx, &capture), http.MethodGet, target, nil)
	if err != nil {
		return ListOutput{}, errors.New("cannot build Vercel Blob list request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Api-Version", Version)
	req.Header.Set("User-Agent", buildinfo.UserAgent())
	h := c.http[(c.next.Add(1)-1)%uint64(len(c.http))]
	done := c.Tally.Attempt()
	resp, err := h.Do(req)
	done()
	if err != nil {
		// net/http puts the full URL in the outer transport error. Keep
		// its type and the underlying Guard or connection cause.
		if e, ok := err.(*url.Error); ok {
			err = &url.Error{Op: e.Op, URL: Endpoint, Err: e.Err}
		}
		return ListOutput{}, objhttp.Timeout(actx, ctx, c.timeout, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ListOutput{}, responseFailure(resp, c.timeout)
	}
	if err := faults.At("response-read"); err != nil {
		return ListOutput{}, err
	}
	return decodePage(capture.Body, limit)
}
