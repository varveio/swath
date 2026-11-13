// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttptest

import (
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
)

// Request renders one request as a client's wire snapshot holds it: the
// method, origin (scheme and the authority it was addressed to) and escaped
// path; each query parameter as sent, in order, duplicates kept; each
// header value, by header name, duplicates kept; and the body's length.
// header maps a header's value to its rendering (a volatile part replaced
// by its form), or drops the header (keep false).
func Request(req *http.Request, origin string, body int, header func(name, value string) (rendered string, keep bool)) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s%s\n", req.Method, origin, req.URL.EscapedPath())
	if req.URL.RawQuery != "" {
		for p := range strings.SplitSeq(req.URL.RawQuery, "&") {
			fmt.Fprintf(&b, "  query %s\n", p)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(req.Header)) {
		for _, v := range req.Header[k] {
			if v, keep := header(k, v); keep {
				fmt.Fprintf(&b, "  header %s: %s\n", k, v)
			}
		}
	}
	fmt.Fprintf(&b, "  body %d bytes\n", body)
	return b.String()
}

// Snapshot holds got, the rendering of every request shape a client sends,
// to the checked-in file, edited by hand when the wire is meant to change.
// A difference names its first line and prints the whole wire for review:
// it is SDK drift until the client's semantic request tests say whether it
// is wrong.
func Snapshot(t testing.TB, file, got string) {
	t.Helper()
	want, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if got == string(want) {
		return
	}
	gl, wl := strings.Split(got, "\n"), strings.Split(string(want), "\n")
	i := 0
	for i < len(gl) && i < len(wl) && gl[i] == wl[i] {
		i++
	}
	t.Errorf("the wire differs from %s first at line %d:\n got  %q\n want %q\nthe whole wire now (review it before replacing the file):\n%s",
		file, i+1, line(gl, i), line(wl, i), got)
}

func line(lines []string, i int) string {
	if i < len(lines) {
		return lines[i]
	}
	return "(end)"
}
