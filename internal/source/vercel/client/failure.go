// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/internal/source/objhttp"
)

type answer struct{ failure objhttp.Failure }

func (a answer) Failure() objhttp.Failure { return a.failure }

func (a answer) Error() string {
	f := a.failure
	if f.Code != "" {
		return fmt.Sprintf("Vercel Blob returned HTTP %d (%s)", f.Status, f.Code)
	}
	return fmt.Sprintf("Vercel Blob returned HTTP %d", f.Status)
}

func responseFailure(resp *http.Response, timeout time.Duration) error {
	var w struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	body, _ := io.ReadAll(resp.Body) // the Guard already bounded and read it
	f := objhttp.Failure{Status: resp.StatusCode}
	// Retain only known codes, never service messages, redirect targets,
	// or unknown code text that can echo the token or cursor.
	if json.Unmarshal(body, &w) == nil {
		switch w.Error.Code {
		case "bad_request", "forbidden", "not_allowed", "not_found", "store_not_found", "store_suspended",
			"rate_limited", "service_unavailable", "internal_server_error", "unknown_error":
			f.Code = w.Error.Code
		}
	}

	f.Throttle = f.Status == http.StatusTooManyRequests || f.Code == "rate_limited"
	f.Timeout = !f.Throttle && f.Status == http.StatusRequestTimeout
	f.Transient = f.Throttle || f.Timeout || f.Status >= 500
	if f.Transient {
		f.Wait = retryAfter(resp.Header.Get("Retry-After"), time.Now(), timeout)
	} else {
		switch {
		case f.Status == http.StatusUnauthorized:
			f.Class = objhttp.ClassAuth
		case f.Status == http.StatusForbidden:
			f.Class = objhttp.ClassAccessDenied
		case f.Status == http.StatusNotFound && f.Code == "store_not_found":
			f.Class = objhttp.ClassNotFound
		}
	}
	return answer{f}
}

func retryAfter(value string, now time.Time, cap time.Duration) time.Duration {
	value = strings.TrimSpace(value)
	if value != "" && strings.Trim(value, "0123456789") == "" {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seconds > uint64(cap/time.Second) {
			return cap
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	return min(max(0, when.Sub(now)), cap)
}
