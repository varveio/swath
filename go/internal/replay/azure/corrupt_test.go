// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure_test

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// withBody replaces an answer's body and keeps its Content-Length true.
func withBody(e exchange, body string) exchange {
	h := map[string][]string{}
	for k, v := range e.headers {
		h[k] = v
	}
	if _, ok := h["content-length"]; ok {
		h["content-length"] = []string{strconv.Itoa(len(body))}
	}
	return exchange{e.status, h, []byte(body)}
}

func onFirst(edit func(string) string) func([]exchange) []exchange {
	return func(pages []exchange) []exchange {
		pages[0] = withBody(pages[0], edit(string(pages[0].body)))
		return pages
	}
}

func replaceOnce(body, from, to string) string {
	if !strings.Contains(body, from) {
		panic("mutation target " + from + " is absent")
	}
	return strings.Replace(body, from, to, 1)
}

var blobRe = regexp.MustCompile(`(?s)<Blob>.*?</Blob>`)

// entries edits the ordered Blob elements of a page.
func entries(edit func([]string) []string) func(string) string {
	return func(body string) string {
		loc := blobRe.FindAllStringIndex(body, -1)
		if len(loc) < 2 {
			panic("mutation needs two entries")
		}
		var items []string
		for _, l := range loc {
			items = append(items, body[l[0]:l[1]])
		}
		start, end := loc[0][0], loc[len(loc)-1][1]
		if body[start:end] != strings.Join(items, "") {
			panic("entries are not contiguous")
		}
		return body[:start] + strings.Join(edit(items), "") + body[end:]
	}
}

// mutations are the reference test's corruptions of a matched replay
// answer, plus three of the XML policy: a duplicated root attribute, a
// comment inside a Name, and a no-break space between elements.
var mutations = map[string]func([]exchange) []exchange{
	"drop-first":     onFirst(entries(func(l []string) []string { return l[1:] })),
	"duplicate-last": onFirst(entries(func(l []string) []string { return append(l, l[len(l)-1]) })),
	"swap-order-pair": onFirst(entries(func(l []string) []string {
		n := len(l) - 1
		l[n], l[n-1] = l[n-1], l[n]
		return l
	})),
	"double-decode": onFirst(func(b string) string { return replaceOnce(b, "a%2Bb", "a+b") }),
	"terminate-early": func(pages []exchange) []exchange {
		pages = pages[:len(pages)-1]
		last := len(pages) - 1
		pages[last] = withBody(pages[last], regexp.MustCompile(`<NextMarker>[^<]*</NextMarker>`).ReplaceAllLiteralString(string(pages[last].body), "<NextMarker />"))
		return pages
	},
	"repeat-prefix": func(pages []exchange) []exchange {
		m := regexp.MustCompile(`(?s)<BlobPrefix>.*?</BlobPrefix>`).FindString(string(pages[0].body))
		if m == "" {
			panic("first page has no prefix to repeat")
		}
		pages[1] = withBody(pages[1], strings.Replace(string(pages[1].body), "</Blobs>", m+"</Blobs>", 1))
		return pages
	},
	"wrong-status": func(pages []exchange) []exchange {
		s := 400
		if pages[0].status == 400 {
			s = 409
		}
		return []exchange{{s, pages[0].headers, pages[0].body}}
	},
	"wrong-error": onFirst(func(b string) string {
		return regexp.MustCompile(`<Code>[A-Za-z]+</Code>`).ReplaceAllLiteralString(b, "<Code>InternalError</Code>")
	}),
	"duplicate-attribute": onFirst(func(b string) string {
		return replaceOnce(b, `ContainerName="replay-bucket"`, `ContainerName="replay-bucket" ContainerName="wrong"`)
	}),
	"comment-in-name": onFirst(func(b string) string {
		i := strings.Index(b, "<Name>") + len("<Name>")
		return b[:i+1] + "<!--corrupt-->" + b[i+1:]
	}),
	"reference-after-root": onFirst(func(b string) string { return replaceOnce(b, "</EnumerationResults>", "</EnumerationResults>&#32;") }),
	"reference-before-root": onFirst(func(b string) string {
		return replaceOnce(b, "<EnumerationResults", "&#10;<EnumerationResults")
	}),
	"nbsp-between-elements": onFirst(func(b string) string { return replaceOnce(b, "</Blob><Blob>", "</Blob>\u00a0<Blob>") }),
}

// TestEvidenceRejectsCorruptedReplayResults corrupts replay answers that
// matched their native capture; every corruption must be rejected, so a
// comparator that tolerates a lost, repeated or reordered name, a
// re-decoded name, an early end, a repeated prefix, a wrong status or
// error code, or XML the reference parser refuses or reads differently is
// caught. It needs only the committed archive (the corpus is served from
// the manifest in memory).
func TestEvidenceRejectsCorruptedReplayResults(t *testing.T) {
	ev := openEvidence(t, "")
	for _, c := range []struct{ key, mutation string }{
		{"azure-03a~2026-06-06", "drop-first"}, {"azure-03a~2026-06-06", "duplicate-last"},
		{"azure-03a~2026-06-06", "swap-order-pair"}, {"azure-07o~2026-06-06", "double-decode"},
		{"azure-09q~2026-06-06", "terminate-early"}, {"azure-09c~2026-06-06", "repeat-prefix"},
		{"azure-08f~2026-06-06", "wrong-status"}, {"azure-08f~2026-06-06", "wrong-error"},
		{"azure-03a~2026-06-06", "duplicate-attribute"}, {"azure-03a~2026-06-06", "comment-in-name"},
		{"azure-03a~2026-06-06", "nbsp-between-elements"}, {"azure-03a~2026-06-06", "reference-after-root"},
		{"azure-03a~2026-06-06", "reference-before-root"},
	} {
		file, sdk := ev.capture(t, c.key)
		ev.endpoint.mutate = nil
		if err := catch(func() { ev.endpoint.verify(t, c.key, file, sdk) }); err != nil {
			t.Fatalf("%s does not match before corruption: %v", c.key, err)
		}
		ev.endpoint.mutate = mutations[c.mutation]
		err := catch(func() { ev.endpoint.verify(t, c.key, file, sdk) })
		if err == nil {
			t.Errorf("%s with %s accepted", c.key, c.mutation)
		} else {
			t.Logf("%s with %s: %v", c.key, c.mutation, err)
		}
	}
}

// TestPinRules pins the outcome rule: a mismatch pin is matched in full,
// except the two marker-echo pins whose actual value is a prefix.
func TestPinRules(t *testing.T) {
	for _, c := range []struct {
		key, outcome, pin string
		ok                bool
	}{
		{"azure-04k~2026-06-06", "mismatch: Delimiter echo differs: expected=null actual=", "Delimiter echo differs: expected=null actual=", true},
		{"azure-04k~2026-06-06", "mismatch: Delimiter echo differs: expected=null actual=x", "Delimiter echo differs: expected=null actual=", false},
		{"azure-04k~2026-06-06", "mismatch: entry count differs: expected=1 actual=2 Delimiter echo differs: expected=null actual=", "Delimiter echo differs: expected=null actual=", false},
		{"azure-09h~2026-06-06", "mismatch: replay marker echo differs: expected=token-sha256-00 actual=az1.AAB3", "replay marker echo differs: expected=token-sha256-00 actual=az1.", true},
		{"azure-09h~2026-06-06", "mismatch: replay marker echo differs: expected=token-sha256-01 actual=az1.AAB3", "replay marker echo differs: expected=token-sha256-00 actual=az1.", false},
		{"azure-09h~2026-06-06", "match", "replay marker echo differs: expected=token-sha256-00 actual=az1.", false},
	} {
		if got := atPin(c.key, c.outcome, c.pin); got != c.ok {
			t.Errorf("%s %q: %v", c.key, c.outcome, got)
		}
	}
}

// TestPinAdjustments ports the reference's two adjustment rules: archived
// BOM pins expect what remained with the BOM restored, and updates
// replace what that rule leaves.
func TestPinAdjustments(t *testing.T) {
	raw := func(s string) json.RawMessage { b, _ := json.Marshal(s); return b }
	mm := func(s string) json.RawMessage { b, _ := json.Marshal(map[string]string{"mismatch": s}); return b }
	archived := map[string]json.RawMessage{
		"only-bom":     mm(archivedBOMPinPrefix + "nothing else"),
		"bom-and-echo": mm(archivedBOMPinPrefix + "Delimiter echo differs: expected=null actual="),
		"status":       mm("Azure HTTP status differs: expected=200 actual=400"),
		"matched":      raw("match"),
	}
	got, err := adjustPins(archived, nil)
	if err != nil || !got["only-bom"].match || got["bom-and-echo"].mismatch != "Delimiter echo differs: expected=null actual=" ||
		got["status"].mismatch != "Azure HTTP status differs: expected=200 actual=400" || !got["matched"].match {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := adjustPins(map[string]json.RawMessage{"bom": mm(archivedBOMPinPrefix)}, nil); err == nil ||
		err.Error() != "archived BOM pin names no remainder" {
		t.Errorf("%v", err)
	}
	got, err = adjustPins(archived, map[string]json.RawMessage{"bom-and-echo": raw("match"), "status": raw("match")})
	if err != nil || !got["bom-and-echo"].match || !got["status"].match || !got["only-bom"].match {
		t.Errorf("%+v %v", got, err)
	}
	if _, err := adjustPins(archived, map[string]json.RawMessage{"only-bom": raw("match")}); err == nil ||
		err.Error() != "expectation update for only-bom repeats the expectation it replaces" {
		t.Errorf("%v", err)
	}
	if _, err := adjustPins(archived, map[string]json.RawMessage{"unknown": raw("match")}); err == nil ||
		err.Error() != "expectation update for unknown, which the archive does not pin" {
		t.Errorf("%v", err)
	}
}

// TestLiteralWhiteSpaceOutsideTheRootIsAccepted is the counterpart of the
// reference mutations: literal spaces, tabs and line breaks before and
// after the root leave a matched answer matched.
func TestLiteralWhiteSpaceOutsideTheRootIsAccepted(t *testing.T) {
	ev := openEvidence(t, "")
	file, sdk := ev.capture(t, "azure-03a~2026-06-06")
	for name, edit := range map[string]func(string) string{
		"after root": func(b string) string {
			return replaceOnce(b, "</EnumerationResults>", "</EnumerationResults>\n \t\r\n")
		},
		"before root": func(b string) string { return replaceOnce(b, "<EnumerationResults", " \n\t<EnumerationResults") },
	} {
		ev.endpoint.mutate = onFirst(edit)
		if err := catch(func() { ev.endpoint.verify(t, "azure-03a~2026-06-06", file, sdk) }); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	ev.endpoint.mutate = nil
}
