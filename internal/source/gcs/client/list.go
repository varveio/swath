// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"google.golang.org/api/googleapi"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/objfile"
)

// List sends one objects.list request per attempt, retrying retryable
// failures (objhttp.Do). Each attempt is reported to ctx's
// objfile.Observer as a list request of in.Prefix.
func (c *Client) List(ctx context.Context, in *ListInput) (*Page, error) {
	r := &objfile.Request{Kind: objfile.ListRequest, Name: in.Prefix}
	return objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (*Page, error) {
		return exchange(c, ctx, r, func(svc *storage.Service, actx context.Context) caller[*storage.Objects] {
			return c.listCall(svc, actx, in)
		}, page)
	})
}

// selector is the partial-response selector a listing sends (versions: a
// generations listing's), on Google's endpoint only: emulators and the
// replay server refuse or ignore one; "" elsewhere.
func (c *Client) selector(versions bool) string {
	switch {
	case c.endpoint != DefaultEndpoint:
		return ""
	case versions:
		return versionFields
	}
	return fields
}

// listCall is in's objects.list request on svc, sent on actx.
func (c *Client) listCall(svc *storage.Service, actx context.Context, in *ListInput) *storage.ObjectsListCall {
	call := svc.Objects.List(in.Bucket).Projection("noAcl").Context(actx)
	if f := c.selector(in.Versions); f != "" {
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
	if p := c.cfg.UserProject; p != "" {
		call.UserProject(p)
	}
	if in.MaxResults > 0 {
		call.MaxResults(int64(in.MaxResults))
	}
	return call
}

// caller is one request of the generated client, built and not yet sent.
type caller[T any] interface {
	Header() http.Header
	Do(opts ...googleapi.CallOption) (T, error)
}

// exchange is one attempt of a request: build makes it on the next pooled
// service, to be sent on the context it is given (the attempt's deadline,
// with the Guard's capture); parse reads the decoded 200. The attempt
// carries swath's User-Agent and the credentials' token, is counted, and
// is reported to ctx's objfile.Observer as r (observe). A failure is the
// answer as an *Error, or one this client's failure resolves.
func exchange[T, R any](c *Client, ctx context.Context, r *objfile.Request, build func(svc *storage.Service, actx context.Context) caller[T], parse func(T) (R, error)) (R, error) {
	var zero R
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	var s objhttp.Capture
	call := build(c.svc[c.pick()], objhttp.WithCapture(actx, &s))
	h := call.Header()
	h.Set("User-Agent", buildinfo.UserAgent()+" "+googleapi.UserAgent)
	tok, err := objhttp.WaitToken(actx, c.ts, c.source, c.discovered, c.cfg.RequestTimeout, transientToken)
	if err != nil {
		return zero, err
	}
	if tok != nil {
		h.Set("Authorization", tok.Type()+" "+tok.AccessToken)
	}

	end := observe(ctx, r)
	done := c.Tally.Attempt()
	out, err := call.Do()
	done()
	v, err := outcome(c, actx, ctx, &s, out, err, parse)
	end(int64(len(s.Body)), err)
	return v, err
}

// outcome reads an attempt's answer: the decoded 200, parsed, or the
// failure.
func outcome[T, R any](c *Client, actx, ctx context.Context, s *objhttp.Capture, out T, err error, parse func(T) (R, error)) (R, error) {
	var zero R
	switch {
	case err != nil:
		return zero, c.failure(actx, ctx, s, err)
	case s.Status != http.StatusOK:
		// any 2xx is the generated client's success; only a 200 is the
		// resource asked for, any other status an answer
		return zero, responseError(s.Status, s.Body)
	}
	if err := faults.At("response-read"); err != nil {
		return zero, err
	}
	return parse(out)
}

// observe reports an attempt of r to ctx's objfile.Observer, when it
// carries one, and returns what reports the attempt's end, with the bytes
// of the answer read and its failure, redacted. Every attempt after the
// first is a retry.
func observe(ctx context.Context, r *objfile.Request) func(bytes int64, err error) {
	o := objfile.ObserverFromContext(ctx)
	if o == nil {
		return func(int64, error) {}
	}

	end := o.Begin(*r)
	r.Retry = true
	return func(bytes int64, err error) {
		if end != nil {
			end(objfile.RequestResult{Bytes: bytes, Err: objfile.RedactError(err)})
		}
	}
}

// validate rejects a 200 whose body carries a top-level error member
// (error:null included), which the generated decoder would drop, reading
// the envelope as an empty final page. It parses only a body that can hold
// one: any backslash (an escaped key), or "error" in any letter case (the
// decoder matches keys case-insensitively); without either, no key can
// decode as error, so the pre-filter has no false negatives. A body that
// does not parse is the decoder's to refuse.
func validate(_ *http.Request, status int, _ http.Header, body []byte) error {
	if status != http.StatusOK || !mayHoldError(body) {
		return nil
	}
	var w struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &w) == nil && w.Error != nil {
		return fmt.Errorf("%w: a 200 carrying an error envelope: %.200s", objhttp.ErrSyntax, w.Error)
	}
	return nil
}

// mayHoldError reports a backslash or "error" (quoted, any case) in body.
func mayHoldError(body []byte) bool {
	if bytes.IndexByte(body, '\\') >= 0 {
		return true
	}
	const key = `"error"`
	for i := 0; ; {
		j := bytes.IndexByte(body[i:], '"')
		if j < 0 || i+j+len(key) > len(body) {
			return false
		}
		i += j
		if bytes.EqualFold(body[i:i+len(key)], []byte(key)) {
			return true
		}
		i++
	}
}

// failure is the error an attempt that did not get a page returns: the
// service's answer as an *Error (by the HTTP status sent, which a
// googleapi.Error's body-supplied code may contradict; another 2xx too), a
// 200 body the generated decoder refused as objhttp.ErrSyntax, the Guard's own failures
// resolved by objhttp.Normalize, an attempt timeout as objhttp.ErrTimeout.
func (c *Client) failure(actx, ctx context.Context, s *objhttp.Capture, err error) error {
	var ge *googleapi.Error
	switch {
	case errors.As(err, &ge) && s.Status != 0:
		e := &Error{Status: s.Status, Message: ge.Message}
		if len(ge.Errors) > 0 {
			e.Reason = ge.Errors[0].Reason
		}
		if loc := ge.Header.Get("Location"); e.Status/100 == 3 && e.Message == "" {
			e.Message = "redirect to " + loc + " (not followed)"
		}
		if e.Status == http.StatusBadRequest {
			e.Query = s.URL.RawQuery // a malformed request names itself
		}
		if e.Message == "" && e.Reason == "" && s.URL != nil {
			// No error envelope (a proxy's or a wrong endpoint's answer):
			// the status's text and the path asked for say what failed.
			e.Message = http.StatusText(e.Status) + " for " + s.URL.Path
		}
		return e
	case s.Status == http.StatusOK:
		return fmt.Errorf("%w: %w", objhttp.ErrSyntax, err)
	case s.Status/100 == 2:
		return responseError(s.Status, s.Body)
	}
	if errors.Is(err, objhttp.ErrSyntax) {
		return err
	}
	return objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, err)
}

// responseError is the answer a 2xx other than 200 and its body make: the
// error envelope's message and first reason, when the body holds one.
func responseError(status int, body []byte) *Error {
	e := &Error{Status: status}
	var w struct {
		Error struct {
			Message string `json:"message"`
			Errors  []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &w) == nil {
		e.Message = w.Error.Message
		if len(w.Error.Errors) > 0 {
			e.Reason = w.Error.Errors[0].Reason
		}
	}
	return e
}

// page converts a decoded 200 strictly: an objects envelope (kind), every
// item present, sizes that fit int64, RFC 3339 times.
func page(out *storage.Objects) (*Page, error) {
	switch {
	case out == nil:
		return nil, fmt.Errorf("%w: a 200 whose body is null", objhttp.ErrSyntax)
	case out.Kind != "storage#objects":
		return nil, fmt.Errorf("%w: a 200 that is not an objects envelope (kind %q)", objhttp.ErrSyntax, out.Kind)
	}
	p := &Page{Prefixes: out.Prefixes, NextPageToken: out.NextPageToken, Items: make([]Object, len(out.Items))}
	for i, o := range out.Items {
		if o == nil {
			return nil, fmt.Errorf("%w: item %d is null", objhttp.ErrSyntax, i)
		}
		var err error
		if p.Items[i], err = item(o); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// item converts one decoded object resource strictly: a size that fits
// int64, RFC 3339 times.
func item(o *storage.Object) (Object, error) {
	if o.Size > math.MaxInt64 {
		return Object{}, fmt.Errorf("%w: size %d", objhttp.ErrSyntax, o.Size)
	}
	x := Object{Name: o.Name, Size: int64(o.Size), ETag: o.Etag, Generation: o.Generation, Metageneration: o.Metageneration,
		StorageClass: o.StorageClass, MD5: o.Md5Hash, CRC32C: o.Crc32c, ContentType: o.ContentType}
	var err error
	if x.Updated, err = micros("updated", o.Updated); err != nil {
		return Object{}, err
	}
	if x.TimeCreated, err = micros("timeCreated", o.TimeCreated); err != nil {
		return Object{}, err
	}
	if x.TimeDeleted, err = micros("timeDeleted", o.TimeDeleted); err != nil {
		return Object{}, err
	}
	return x, nil
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
