// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"
)

// The replay wire contract rejects some request targets in its HTTP layer,
// before any route sees them, with 400 and an HTML error page rather than
// an S3 error: a fragment, an ambiguous path (an empty segment, an encoded
// '/', '%' or dot segment, a dot segment with a parameter), a suspicious or
// illegal path character, a malformed escape or invalid UTF-8 once decoded,
// a path that climbs above the root, and a target that is neither a path
// nor an absolute URI. targetProblem names the problem, or returns "" for
// a target the routes may answer. The status and headers are the
// contract's; the page itself is this server's own, not a copy of the
// contract's HTTP layer's page (TestRejectsAmbiguousTargets, and the
// driver's raw-target cases, which compare status and headers only).
func targetProblem(target string) string {
	_, problem := targetPath(target)
	return problem
}

// targetPath returns the raw path of a request target and its problem:
// the target up to its query for an origin-form target ("/..."), or the
// part after the scheme and authority for an absolute one
// ("http://host/...", whose authority may carry no user information). A
// target that is neither is a problem. The contract's HTTP layer also
// refuses an absolute target whose authority differs from the Host header;
// Go's HTTP layer drops the Host header for such a request, as RFC 9112
// section 3.2.2 has an origin server ignore it, so this server serves it.
func targetPath(target string) (string, string) {
	if strings.IndexByte(target, '#') >= 0 {
		return "", "Fragment"
	}
	path := target
	if !strings.HasPrefix(path, "/") {
		scheme, rest, ok := strings.Cut(path, ":")
		if !ok || !validScheme(scheme) {
			return "", "Bad Request"
		}
		if strings.HasPrefix(rest, "//") {
			authority := rest[2:]
			end := strings.IndexAny(authority, "/?")
			if end < 0 {
				end = len(authority)
			}
			authority, rest = authority[:end], authority[end:]
			if strings.IndexByte(authority, '@') >= 0 {
				return "", "Deprecated User Info"
			}
			if rest == "" || rest[0] == '?' {
				rest = "/" + rest // an empty path is the root
			}
		}
		path = rest
	}
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	if path == "" || path[0] != '/' {
		return path, ""
	}
	return path, pathProblem(path)
}

func validScheme(s string) bool {
	if s == "" || !(s[0] >= 'a' && s[0] <= 'z' || s[0] >= 'A' && s[0] <= 'Z') {
		return false
	}
	for i := 1; i < len(s); i++ {
		if c := s[i]; !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

// pathProblem checks an origin-form path.
func pathProblem(path string) string {
	var problems []string
	note := func(p string) {
		for _, q := range problems {
			if q == p {
				return
			}
		}
		problems = append(problems, p)
	}
	// Raw characters: what a path may carry unescaped.
	for i := 0; i < len(path); i++ {
		switch c := path[i]; {
		case c == '\\':
			note("Suspicious Path Character")
			note("Illegal Path Character")
		case !pathChar(c):
			note("Illegal Path Character")
		}
	}
	segments := strings.Split(path, "/")[1:]
	depth := 0
	for n, seg := range segments {
		name, _, param := strings.Cut(seg, ";")
		decoded, problem := decodeSegment(name)
		if problem == "Bad Request" {
			return problem
		}
		if problem != "" {
			note(problem)
		}
		_, ownProblem := decodeSegment(seg[len(name):]) // escapes in the parameters
		if ownProblem == "Bad Request" {
			return ownProblem
		}
		if ownProblem != "" {
			note(ownProblem)
		}
		dot := decoded == "." || decoded == ".."
		switch {
		case name == "" && n < len(segments)-1:
			note("Ambiguous URI empty segment")
		case dot && name != decoded:
			note("Ambiguous URI path segment")
		case dot && param:
			note("Ambiguous URI path parameter")
		}
		switch decoded {
		case ".":
		case "..":
			if depth == 0 {
				return "Bad Request"
			}
			depth--
		default:
			if n < len(segments)-1 || name != "" {
				depth++
			}
		}
	}
	return strings.Join(problems, ", ")
}

// pathChar reports a byte a path may carry unescaped: unreserved,
// sub-delims, ':', '@', '/' and '%'.
func pathChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("-._~!$&'()*+,;=:@/%", c) >= 0
}

// decodeSegment percent-decodes one raw segment and names what decoding
// found wrong: a malformed escape or a decoded NUL ("Bad Request"), a
// %uXXXX escape, invalid UTF-8, an encoded '/' or '%', or an encoded
// control character or '\'.
func decodeSegment(raw string) (string, string) {
	if strings.IndexByte(raw, '%') < 0 {
		return raw, ""
	}
	var b strings.Builder
	problem := ""
	for i := 0; i < len(raw); i++ {
		if raw[i] != '%' {
			b.WriteByte(raw[i])
			continue
		}
		if i+1 < len(raw) && raw[i+1] == 'u' {
			if problem == "" {
				problem = "UTF-16 encoding"
			}
			i++
			continue
		}
		if i+2 >= len(raw) {
			return "", "Bad Request"
		}
		v, err := strconv.ParseUint(raw[i+1:i+3], 16, 8)
		if err != nil {
			return "", "Bad Request"
		}
		i += 2
		switch c := byte(v); {
		case c == 0:
			return "", "Bad Request"
		case c == '/':
			problem = "Ambiguous URI path separator"
		case c == '%':
			problem = "Ambiguous URI path encoding"
		case c < 0x20 || c == 0x7f || c == '\\':
			if problem == "" {
				problem = "Suspicious Path Character"
			}
		}
		b.WriteByte(byte(v))
	}
	if !utf8.ValidString(b.String()) && problem == "" {
		problem = "Bad UTF-8 encoding"
	}
	return b.String(), problem
}

// rejectTarget answers a target targetProblem refused, as the contract's
// HTTP layer does: 400, an HTML page, no caching, the connection closed,
// and nothing counted by the replay meters.
func rejectTarget(w http.ResponseWriter, r *http.Request, problem string) {
	rejectRequest(w, r, http.StatusBadRequest, problem)
}

// rejectRequest answers a request the contract's HTTP layer refuses
// before routing, with its status, an HTML page naming the problem, no
// caching, and the connection closed.
func rejectRequest(w http.ResponseWriter, r *http.Request, status int, problem string) {
	code := strconv.Itoa(status)
	body := "<html>\n<head>\n<title>Error " + code + " " + htmlEscape(problem) + "</title>\n</head>\n<body>\n<h2>HTTP ERROR " + code + " " +
		htmlEscape(problem) + "</h2>\n</body>\n</html>\n"
	h := w.Header()
	h["Cache-Control"] = []string{"must-revalidate,no-cache,no-store"}
	h["Content-Type"] = []string{"text/html;charset=iso-8859-1"}
	h["Content-Length"] = []string{strconv.Itoa(len(body))}
	h["Connection"] = []string{"close"}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(body))
	}
}

// The request-head limits, the contract's: 8 KiB with the S3 route alone
// (its HTTP layer's default), 64 KiB when a native route is enabled.
const (
	headLimitS3     = 8 << 10
	headLimitNative = 64 << 10
)

// headLimit is the largest request head (request line, headers and the
// blank line) the server reads for cfg.
func (c *Config) headLimit() int {
	if p := c.protocols(); len(p) == 1 && p[0] == ProtocolS3 {
		return headLimitS3
	}
	return headLimitNative
}

// maxHeaderBytes is http.Server.MaxHeaderBytes: twice the larger head
// limit (128 KiB), so a head up to that size (plus net/http's 4 KiB of
// slack) reaches headProblem and gets the contract's answer; net/http
// refuses a larger one itself (431 with its own text/plain page), one of
// the parser-level residuals.
func (c *Config) maxHeaderBytes() int { return 2 * headLimitNative }

// headProblem measures a request head against limit as the contract's
// HTTP layer does (established by sending growing heads to the Java
// server): a request line longer than the limit is 414 URI Too Long; a
// head of more than limit bytes is 431 Request Header Fields Too Large.
// The head is measured as it was sent in the usual form ("Name: value"
// lines); net/http keeps no raw copy, so optional whitespace around a
// value is not counted.
func headProblem(r *http.Request, limit int) (int, string) {
	line := len(r.Method) + 1 + len(r.RequestURI) + 1 + len(r.Proto) + 2
	if line > limit {
		return http.StatusRequestURITooLong, "URI Too Long"
	}
	head := line + len("Host: ") + len(r.Host) + 2 + 2
	for name, values := range r.Header {
		for _, v := range values {
			head += len(name) + 2 + len(v) + 2
		}
	}
	if head > limit {
		return http.StatusRequestHeaderFieldsTooLarge, "Request Header Fields Too Large"
	}
	return 0, ""
}

func htmlEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
