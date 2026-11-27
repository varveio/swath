// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DefaultRequestTimeout bounds one object-store request attempt.
const DefaultRequestTimeout = 30 * time.Second

// Days and weeks, the units ParseDuration adds to Go's.
const (
	day  = 24 * time.Hour
	week = 7 * day
)

// ParseDuration is Go's duration syntax (90s, 1h30m) with d (24h) and w
// (7d) besides: 2d, 1w, 1d12h, 1.5d.
func ParseDuration(v string) (time.Duration, error) {
	if !strings.ContainsAny(v, "dw") {
		return time.ParseDuration(v)
	}
	var total time.Duration
	for rest := v; rest != ""; {
		i := strings.IndexFunc(rest, func(r rune) bool { return r != '.' && (r < '0' || r > '9') })
		if i <= 0 {
			return 0, fmt.Errorf("invalid duration %q", v)
		}
		j := i + strings.IndexFunc(rest[i:], func(r rune) bool { return r == '.' || r >= '0' && r <= '9' })
		if j < i {
			j = len(rest)
		}
		num, unit := rest[:i], rest[i:j]
		var d time.Duration
		switch unit {
		case "d", "w":
			u := day
			if unit == "w" {
				u = week
			}
			f, err := strconv.ParseFloat(num, 64)
			x := f * float64(u)
			if err != nil || x >= 1<<63 {
				return 0, fmt.Errorf("invalid duration %q", v)
			}
			d = time.Duration(x)
		default:
			var err error
			if d, err = time.ParseDuration(num + unit); err != nil {
				return 0, fmt.Errorf("invalid duration %q", v)
			}
		}
		if total+d < total {
			return 0, fmt.Errorf("invalid duration %q", v)
		}
		total += d
		rest = rest[j:]
	}
	return total, nil
}

// FormatDuration is d as a user would type it: 10m, 1h30m, 30s, 0.
func FormatDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	if s == "0s" {
		return "0"
	}
	return s
}

// Store is what every object store takes alike (the command's store flag
// group, cli.StoreGroup, fills one). Each provider reads it into its own
// options.
type Store struct {
	// Endpoint replaces the provider's own service (an emulator, a
	// compatible store); "" is the service itself.
	Endpoint string
	// Timeout bounds each request; one that exceeds it is retried.
	Timeout time.Duration
	// Anonymous sends no credentials: the one way an object store is
	// listed unauthenticated. Without it a source with no usable
	// credentials fails (class auth) before any listing request.
	Anonymous bool
	// Trust is how the service's TLS certificate is verified.
	Trust Trust
}
