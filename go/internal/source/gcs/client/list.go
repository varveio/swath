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

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// List sends one objects.list request per attempt, retrying retryable
// failures (objhttp.Do).
func (c *Client) List(ctx context.Context, in *ListInput) (*Page, error) {
	return objhttp.Do(ctx, c.Retry, &c.Tally, func() (*Page, error) { return c.attempt(ctx, in) })
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

func (c *Client) attempt(ctx context.Context, in *ListInput) (*Page, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	var s objhttp.Capture
	call := c.svc[c.pick()].Objects.List(in.Bucket).Projection("noAcl").Context(objhttp.WithCapture(actx, &s))
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
	h := call.Header()
	h.Set("User-Agent", buildinfo.UserAgent()+" "+googleapi.UserAgent)
	tok, err := objhttp.WaitToken(actx, c.ts, c.source, c.cfg.RequestTimeout, transientToken)
	if err != nil {
		return nil, err
	}
	if tok != nil {
		h.Set("Authorization", tok.Type()+" "+tok.AccessToken)
	}
	done := c.Tally.Attempt()
	out, err := call.Do()
	done()
	if err != nil {
		return nil, c.failure(actx, ctx, &s, err)
	}
	if s.Status != http.StatusOK {
		// any 2xx is the generated client's success; a listing is a 200,
		// any other status an answer
		return nil, responseError(s.Status, s.Body)
	}
	if err := faults.At("response-read"); err != nil {
		return nil, err
	}
	p, err := page(out)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// validate rejects a 200 whose body carries a top-level error member
// (error:null included), which the generated decoder would drop, reading
// the envelope as an empty final page. It parses only a body that can hold
// one: any backslash (an escaped key), or "error" in any letter case (the
// decoder matches keys case-insensitively); without either, no key can
// decode as error, so the pre-filter has no false negatives. A body that
// does not parse is the decoder's to refuse.
func validate(_ *http.Request, status int, body []byte) error {
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
		if o.Size > math.MaxInt64 {
			return nil, fmt.Errorf("%w: size %d", objhttp.ErrSyntax, o.Size)
		}
		x := &p.Items[i]
		*x = Object{Name: o.Name, Size: int64(o.Size), ETag: o.Etag, Generation: o.Generation, Metageneration: o.Metageneration,
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
