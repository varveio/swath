// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

const (
	unsignedPayload = "UNSIGNED-PAYLOAD"
	amzDate         = "20060102T150405Z"
)

// signer signs requests with AWS Signature Version 4 for service s3. The
// signing key depends only on the day, so it is derived once per day.
type signer struct {
	creds  Credentials
	region string

	mu  sync.Mutex
	day string
	key []byte
}

func (s *signer) signingKey(day string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.day != day {
		k := hmacSHA256([]byte("AWS4"+s.creds.SecretAccessKey), day)
		k = hmacSHA256(k, s.region)
		k = hmacSHA256(k, "s3")
		s.day, s.key = day, hmacSHA256(k, "aws4_request")
	}
	return s.key
}

// sign adds the SigV4 headers to a GET request whose URL carries the
// canonical (sorted, RFC 3986 encoded) query string and path.
func (s *signer) sign(req *http.Request, now time.Time) {
	ts := now.UTC().Format(amzDate)
	day := ts[:8]
	req.Header.Set("X-Amz-Date", ts)
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	signed := "host;x-amz-content-sha256;x-amz-date"
	headers := "host:" + req.Host + "\nx-amz-content-sha256:" + unsignedPayload + "\nx-amz-date:" + ts + "\n"
	if s.creds.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.creds.SessionToken)
		signed += ";x-amz-security-token"
		headers += "x-amz-security-token:" + s.creds.SessionToken + "\n"
	}
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := "GET\n" + path + "\n" + req.URL.RawQuery + "\n" + headers + "\n" + signed + "\n" + unsignedPayload
	sum := sha256.Sum256([]byte(canonical))
	scope := day + "/" + s.region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + ts + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	sig := hex.EncodeToString(hmacSHA256(s.signingKey(day), toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.creds.AccessKeyID+"/"+scope+
		", SignedHeaders="+signed+", Signature="+sig)
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// queryEscape is SigV4's URI encoding: everything but A-Z a-z 0-9 - _ . ~
// is percent-encoded with upper-case hex.
func queryEscape(s string) string {
	n := 0
	for i := 0; i < len(s); i++ {
		if !unreserved(s[i]) {
			n++
		}
	}
	if n == 0 {
		return s
	}
	b := make([]byte, 0, len(s)+2*n)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if unreserved(c) {
			b = append(b, c)
		} else {
			b = append(b, '%', "0123456789ABCDEF"[c>>4], "0123456789ABCDEF"[c&15])
		}
	}
	return string(b)
}

func unreserved(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~'
}
