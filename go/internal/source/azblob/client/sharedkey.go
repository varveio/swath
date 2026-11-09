// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// sharedKey signs requests with an account key: the Shared Key scheme of
// service version 2009-09-19 and later (Authorize with Shared Key,
// "Blob, Queue, and File services"). The string to sign is the verb, eleven
// standard headers (Date empty: x-ms-date carries the time), the
// canonicalized x-ms-* headers and the canonicalized resource; the
// signature is base64 HMAC-SHA256 under the decoded key.
type sharedKey struct {
	account string
	key     []byte
}

func newSharedKey(account, key string) (*sharedKey, error) {
	b, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("account key is not base64: %v", err)
	}
	return &sharedKey{account: account, key: b}, nil
}

// sign sets the Authorization header of req, whose x-ms-* headers
// (x-ms-date among them) are already set.
func (k *sharedKey) sign(req *http.Request) error {
	sts, err := k.stringToSign(req)
	if err != nil {
		return err
	}
	h := hmac.New(sha256.New, k.key)
	h.Write([]byte(sts))
	req.Header.Set(headerAuthorization, "SharedKey "+k.account+":"+base64.StdEncoding.EncodeToString(h.Sum(nil)))
	return nil
}

func (k *sharedKey) stringToSign(req *http.Request) (string, error) {
	res, err := k.canonicalResource(req.URL)
	if err != nil {
		return "", err
	}
	length := req.Header.Get("Content-Length")
	if length == "0" {
		length = ""
	}
	h := req.Header.Get
	return strings.Join([]string{req.Method, h("Content-Encoding"), h("Content-Language"), length, h("Content-MD5"),
		h("Content-Type"), "", h("If-Modified-Since"), h("If-Match"), h("If-None-Match"), h("If-Unmodified-Since"),
		h("Range"), canonicalHeaders(req.Header), res}, "\n"), nil
}

// canonicalHeaders are the x-ms-* headers, names lower-cased and sorted,
// one name:value per line. The service sorts with a culture-aware
// comparison that ignores hyphens at first; for the names this client
// sends (x-ms-client-request-id, x-ms-date, x-ms-version: letters, with
// hyphens at the same positions up to the first difference) that is byte
// order (TestSharedKeyMatchesSDK).
func canonicalHeaders(hdr http.Header) string {
	m := map[string][]string{}
	for k, v := range hdr {
		if n := strings.ToLower(strings.TrimSpace(k)); strings.HasPrefix(n, "x-ms-") {
			m[n] = append(m[n], v...)
		}
	}
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	lines := make([]string, len(names))
	for i, n := range names {
		lines[i] = n + ":" + strings.Join(m[n], ",")
	}
	return strings.Join(lines, "\n")
}

// canonicalResource is "/" + account + the escaped path as sent, then
// each query parameter on its own line, name lower-cased: values decoded,
// sorted, comma-joined. On an emulator endpoint the path already starts
// with the account, which then appears twice (/acct/acct/container), as
// the service and Azurite expect.
func (k *sharedKey) canonicalResource(u *url.URL) (string, error) {
	var b strings.Builder
	b.WriteString("/" + k.account)
	if p := u.EscapedPath(); p != "" {
		b.WriteString(p)
	} else {
		b.WriteString("/")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", fmt.Errorf("shared key: query: %v", err)
	}
	names := make([]string, 0, len(q))
	for n := range q {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		vs := q[n]
		sort.Strings(vs)
		b.WriteString("\n" + strings.ToLower(n) + ":" + strings.Join(vs, ","))
	}
	return b.String(), nil
}
