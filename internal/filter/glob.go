// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// A glob (--include, --exclude) matches the whole of its target: the key
// relative to the source's prefix up to its last '/' (Spec.Base). A
// pattern with a '/' is anchored at that root (a leading '/' only
// anchors); one without matches the last segment at any depth. A key
// ending in '/' (a directory's row) matches as the pattern without it. *
// is any run of characters but '/', ? one character but '/', ** any number
// of whole segments (only as a segment of its own; a trailing one, a/**,
// at least one: below a/, not a/ itself), [a-z] one character of
// a class ([!a-z]: one not in it; never '/'), {a,b} one of the
// alternatives (not nested), \c the character c.
//
// globRegexp translates it to an RE2 expression, which matches in time
// linear in the key, newlines included.
func globRegexp(p string) (string, error) {
	row, _, err := globRegexps(p)
	return row, err
}

// globRegexps also derives the directory keys whose descendants a terminal
// recursive segment covers, using the row expression's parser and anchoring.
func globRegexps(p string) (row, descendants string, err error) {
	if p == "" {
		return "", "", errors.New("empty pattern")
	}
	anchored := strings.Contains(p, "/")
	body := strings.TrimPrefix(p, "/")
	if body == "" {
		return "", "", errors.New("nothing after the leading /")
	}
	var b strings.Builder
	b.WriteString(`\A`)
	if !anchored {
		b.WriteString(anySegments)
	}
	inBrace := false
	for i := 0; i < len(body); {
		switch c := body[i]; {
		case c == '*' && i+1 < len(body) && body[i+1] == '*':
			end := i+2 == len(body)
			if inBrace || i > 0 && body[i-1] != '/' || !end && body[i+2] != '/' {
				return "", "", errors.New("** must be a whole segment (a/**/b, **/x, a/**)")
			}
			switch {
			case end && i > 0:
				// a/** is what lies below a/, not the a/ row itself
				// (as gitignore's): --exclude a or a/ drops that row.
				descendants = b.String() + `\z`
				b.WriteString(`(?s:.+)`)
				i += 2
			case end:
				b.WriteString(`(?s:.*)`)
				i += 2
			default:
				b.WriteString(anySegments)
				i += 3
			}
		case c == '*':
			b.WriteString(`[^/]*`)
			i++
		case c == '?':
			b.WriteString(`[^/]`)
			i++
		case c == '[':
			n, err := globClass(&b, body[i:])
			if err != nil {
				return "", "", err
			}
			i += n
		case c == '{':
			if inBrace {
				return "", "", errors.New("nested { is not supported")
			}
			inBrace = true
			b.WriteString(`(?:`)
			i++
		case c == ',' && inBrace:
			b.WriteByte('|')
			i++
		case c == '}':
			if !inBrace {
				return "", "", errors.New("} without {")
			}
			inBrace = false
			b.WriteByte(')')
			i++
		default:
			if c == '\\' {
				if i++; i == len(body) {
					return "", "", errors.New(`trailing \`)
				}
			}
			_, n := utf8.DecodeRuneInString(body[i:])
			b.WriteString(regexp.QuoteMeta(body[i : i+n]))
			i += n
		}
	}
	if inBrace {
		return "", "", errors.New("{ without }")
	}
	if !strings.HasSuffix(body, "/") {
		b.WriteString(`/?`)
	}
	b.WriteString(`\z`)
	return b.String(), descendants, nil
}

// anySegments is ** followed by '/': zero or more whole segments.
const anySegments = `(?:[^/]*/)*`

// globClass writes the class at the start of s ([...]) and returns its
// length. A class never matches '/': a range across it leaves it out, and
// a '/' of its own is refused (it could never match).
func globClass(b *strings.Builder, s string) (int, error) {
	i, neg := 1, false
	if i < len(s) && s[i] == '!' {
		neg, i = true, i+1
	}
	type span struct{ lo, hi rune }
	var spans []span
	one := func() (rune, error) {
		if i < len(s) && s[i] == '\\' {
			i++
		}
		if i >= len(s) {
			return 0, errors.New("[ without ]")
		}
		r, n := utf8.DecodeRuneInString(s[i:])
		i += n
		return r, nil
	}
	for first := true; ; first = false {
		if i >= len(s) {
			return 0, errors.New("[ without ]")
		}
		if s[i] == ']' && !first {
			break
		}
		if strings.HasPrefix(s[i:], "[:") {
			return 0, errors.New("[:class:] is not supported")
		}
		lo, err := one()
		if err != nil {
			return 0, err
		}
		hi := lo
		if i+1 < len(s) && s[i] == '-' && s[i+1] != ']' {
			i++
			if hi, err = one(); err != nil {
				return 0, err
			}
			if hi < lo {
				return 0, fmt.Errorf("range %c-%c is out of order", lo, hi)
			}
		}
		if !neg && lo == '/' && hi == '/' {
			return 0, errors.New("[/] never matches: '/' separates segments")
		}
		if !neg && lo <= '/' && '/' <= hi {
			spans = append(spans, span{lo, '/' - 1}, span{'/' + 1, hi})
		} else {
			spans = append(spans, span{lo, hi})
		}
	}
	b.WriteByte('[')
	if neg {
		b.WriteString(`^/`)
	}
	for _, sp := range spans {
		if sp.lo <= sp.hi {
			fmt.Fprintf(b, `\x{%x}-\x{%x}`, sp.lo, sp.hi)
		}
	}
	b.WriteByte(']')
	return i + 1, nil
}

// globLiteral is the literal every key a glob matches starts with,
// relative to Spec.Base, and whether the glob is anchored (has a '/'):
// one that is not has none. p is valid (globRegexp).
func globLiteral(p string) (string, bool) {
	if !strings.Contains(p, "/") {
		return "", false
	}
	p = strings.TrimPrefix(p, "/")
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		switch p[i] {
		case '*', '?', '[', '{':
			return b.String(), true
		case '\\':
			i++
		}
		b.WriteByte(p[i])
	}
	return b.String(), true
}
