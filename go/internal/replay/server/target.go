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
	body := "<html>\n<head>\n<title>Error 400 " + htmlEscape(problem) + "</title>\n</head>\n<body>\n<h2>HTTP ERROR 400 " +
		htmlEscape(problem) + "</h2>\n</body>\n</html>\n"
	h := w.Header()
	h["Cache-Control"] = []string{"must-revalidate,no-cache,no-store"}
	h["Content-Type"] = []string{"text/html;charset=iso-8859-1"}
	h["Content-Length"] = []string{strconv.Itoa(len(body))}
	h["Connection"] = []string{"close"}
	w.WriteHeader(http.StatusBadRequest)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(body))
	}
}

func htmlEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
