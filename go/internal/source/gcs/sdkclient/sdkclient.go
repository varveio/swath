// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sdkclient lists Cloud Storage buckets through Google's generated
// JSON API client (google.golang.org/api/storage/v1): one Objects.List call
// per attempt, on the hand-rolled client's core (client.Core: endpoint,
// transport pool, credentials, retry policy, counters), so the provider
// sees the same API, routes, errors and counters.
//
// The generated Do sends one request and never retries (objhttp.Do owns
// retries). Every response passes an objhttp.Guard first (16 MiB success
// bodies, 64 KiB error bodies, the original body closed); a 200 must decode
// to an objects envelope (kind storage#objects), so an empty, null or error
// body is never read as an empty final page.
package sdkclient

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// Client lists Cloud Storage buckets; safe for concurrent use.
type Client struct {
	*client.Core
	svc []*storage.Service // one per pooled transport (Core.Next)
}

// New builds the client on client.NewCore (the same credentials, pool and
// retry policy as client.New).
func New(ctx context.Context, cfg client.Config) (*Client, error) {
	core, err := client.NewCore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c := &Client{Core: core}
	for _, hc := range core.Pool() {
		g := *hc // keeps the pool's no-redirect policy
		g.Transport = record{&objhttp.Guard{Next: hc.Transport}}
		svc, err := storage.NewService(ctx, option.WithHTTPClient(&g), option.WithEndpoint(core.Endpoint()), option.WithoutAuthentication())
		if err != nil {
			return nil, err
		}
		c.svc = append(c.svc, svc)
	}
	return c, nil
}

// List sends one objects.list request per attempt, retrying retryable
// failures (objhttp.Do).
func (c *Client) List(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	return c.Do(ctx, func() (*client.Page, error) { return c.attempt(ctx, in) })
}

func (c *Client) attempt(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	actx, cancel := context.WithTimeout(ctx, c.Timeout())
	defer cancel()
	var s sent
	call := c.svc[c.Next()].Objects.List(in.Bucket).Projection("noAcl").Context(context.WithValue(actx, sentKey{}, &s))
	if f := c.Fields(in.Versions); f != "" {
		call.Fields(googleapi.Field(f))
	}
	if in.Versions {
		call.Versions(true)
	}
	if in.Prefix != "" {
		call.Prefix(in.Prefix)
	}
	if in.Delimiter != "" {
		call.Delimiter(in.Delimiter)
	}
	if in.StartOffset != "" {
		call.StartOffset(in.StartOffset)
	}
	if in.EndOffset != "" {
		call.EndOffset(in.EndOffset)
	}
	if in.PageToken != "" {
		call.PageToken(in.PageToken)
	}
	if p := c.UserProject(); p != "" {
		call.UserProject(p)
	}
	if in.MaxResults > 0 {
		call.MaxResults(int64(in.MaxResults))
	}
	h := call.Header()
	h.Set("User-Agent", buildinfo.Tool)
	tok, err := c.Token(actx)
	if err != nil {
		return nil, err
	}
	if tok != nil {
		h.Set("Authorization", tok.Type()+" "+tok.AccessToken)
	}
	done := c.Attempt()
	out, err := call.Do()
	done()
	if err != nil {
		return nil, c.failure(actx, ctx, &s, err)
	}
	if s.status != http.StatusOK {
		// any 2xx is the generated client's success; a listing is a 200
		return nil, c.syntax(fmt.Errorf("%w: a %d answer", objhttp.ErrSyntax, s.status))
	}
	if err := faults.At("response-read"); err != nil {
		return nil, err
	}
	p, err := page(out)
	if err != nil {
		return nil, c.syntax(err)
	}
	return p, nil
}

// failure is the error an attempt that did not get a page returns: the
// service's answer as a *client.Error (by the HTTP status sent, which a
// googleapi.Error's body-supplied code may contradict), a 2xx body the
// generated decoder refused as ErrSyntax, the Guard's own failures
// unwrapped (objhttp.Cause), an attempt timeout as objhttp.ErrTimeout.
func (c *Client) failure(actx, ctx context.Context, s *sent, err error) error {
	var ge *googleapi.Error
	switch {
	case errors.As(err, &ge) && s.status != 0:
		e := &client.Error{Status: s.status, Message: ge.Message}
		if len(ge.Errors) > 0 {
			e.Reason = ge.Errors[0].Reason
		}
		if loc := ge.Header.Get("Location"); e.Status/100 == 3 && e.Message == "" {
			e.Message = "redirect to " + loc + " (not followed)"
		}
		if e.Status == http.StatusBadRequest {
			e.Query = s.query // a malformed request names itself
		}
		return e
	case s.status/100 == 2:
		return c.syntax(fmt.Errorf("%w: %w", objhttp.ErrSyntax, err))
	}
	err = objhttp.Cause(err)
	if errors.Is(err, objhttp.ErrSyntax) {
		return c.syntax(err)
	}
	return objhttp.Timeout(actx, ctx, c.Timeout(), err)
}

// syntax counts a parse failure and returns err.
func (c *Client) syntax(err error) error {
	c.ParseFailure()
	return err
}

// page converts a decoded 200 strictly: an objects envelope (kind), every
// item present, sizes that fit int64, RFC 3339 times.
func page(out *storage.Objects) (*client.Page, error) {
	switch {
	case out == nil:
		return nil, fmt.Errorf("%w: a 200 whose body is null", objhttp.ErrSyntax)
	case out.Kind != "storage#objects":
		return nil, fmt.Errorf("%w: a 200 that is not an objects envelope (kind %q)", objhttp.ErrSyntax, out.Kind)
	}
	p := &client.Page{Prefixes: out.Prefixes, NextPageToken: out.NextPageToken, Items: make([]client.Object, len(out.Items))}
	for i, o := range out.Items {
		if o == nil {
			return nil, fmt.Errorf("%w: item %d is null", objhttp.ErrSyntax, i)
		}
		if o.Size > math.MaxInt64 {
			return nil, fmt.Errorf("%w: size %d", objhttp.ErrSyntax, o.Size)
		}
		x := &p.Items[i]
		*x = client.Object{Name: o.Name, Size: int64(o.Size), ETag: o.Etag, Generation: o.Generation, Metageneration: o.Metageneration,
			StorageClass: o.StorageClass, MD5: o.Md5Hash, CRC32C: o.Crc32c, ContentType: o.ContentType}
		var err error
		if x.Updated, err = micros("updated", o.Updated); err != nil {
			return nil, err
		}
		if x.TimeCreated, err = micros("timeCreated", o.TimeCreated); err != nil {
			return nil, err
		}
		if x.TimeDeleted, err = micros("timeDeleted", o.TimeDeleted); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// micros reads field's RFC 3339 time as Unix microseconds; 0 when absent.
func micros(field, s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %q", objhttp.ErrSyntax, field, s)
	}
	return t.UnixMicro(), nil
}

// sent is what an attempt's request met on the wire: the status of the
// response handed to the generated client (0 without one) and the query
// it sent.
type sent struct {
	status int
	query  string
}

type sentKey struct{}

// record notes the request's query and the (guarded) response's status in
// the attempt's sent, from the request context.
type record struct{ next http.RoundTripper }

func (r record) RoundTrip(req *http.Request) (*http.Response, error) {
	s, _ := req.Context().Value(sentKey{}).(*sent)
	if s != nil {
		s.query = req.URL.RawQuery
	}
	resp, err := r.next.RoundTrip(req)
	if s != nil && resp != nil {
		s.status = resp.StatusCode
	}
	return resp, err
}
