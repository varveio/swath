// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"net/url"
	"strings"
)

// Backblaze reports whether endpoint is Backblaze B2's S3-compatible API
// (s3.REGION.backblazeb2.com, or a bucket's virtual host under it). B2
// differs from S3 where swath must know it: its ListObjectVersions pages
// (internal/source/s3, b2.go), and writes, which take no condition, so a
// dataset cannot be committed there (internal/inventory).
func Backblaze(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	host := endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		host = u.Hostname()
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return strings.HasSuffix(host, ".backblazeb2.com")
}
