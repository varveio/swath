// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package output

import (
	"bytes"
	"strings"
	"testing"
)

func body(o *Output) []byte {
	var b bytes.Buffer
	for _, c := range o.Body() {
		b.Write(c)
	}
	return b.Bytes()
}

// referenceEscape is XMLEntity applied byte by byte, the definition the
// chunked writer must equal.
func referenceEscape(s []byte) []byte {
	var out []byte
	for _, c := range s {
		if e := XMLEntity[c]; e != "" {
			out = append(out, e...)
		} else {
			out = append(out, c)
		}
	}
	return out
}

// FuzzEscaped holds that Escaped and EscapedBytes write XMLEntity's bytes
// whatever the chunk size, so no entity or multi-byte character is changed
// by a chunk edge.
func FuzzEscaped(f *testing.F) {
	for _, s := range []string{"", "plain/key", "a&b<c>d\"e'f", "\x00\x01\t\n\r\x1f\x7f", "k\xed\xa0\x80z\xff", "\U0001F600 é ", strings.Repeat("&", 300)} {
		f.Add([]byte(s), 7)
	}
	f.Fuzz(func(t *testing.T, s []byte, chunk int) {
		chunk = 1 + (chunk&0x7fffffff)%64
		want := referenceEscape(s)
		for _, fromString := range []bool{false, true} {
			o, err := NewChunked(NewBudget(1<<30, nil), 1, 1<<30, chunk, nil)
			if err != nil {
				t.Fatal(err)
			}
			if fromString {
				o.Escaped(string(s))
			} else {
				o.EscapedBytes(s)
			}
			if got := body(o); !bytes.Equal(got, want) || o.Len() != len(want) {
				t.Fatalf("chunk %d string=%v: %q, want %q", chunk, fromString, got, want)
			}
			o.Close()
		}
	})
}

// FuzzPercentEncoded holds that PercentEncoded equals AppendPercentEncoded
// whatever the room left in the chunk (the one-pass and the admitted
// paths), and that the output decodes back to the input under S3's table
// ('+' a space, %XX a byte).
func FuzzPercentEncoded(f *testing.F) {
	for _, s := range []string{"", "a b+c", "logs/alpha/dt=2026-01-01/part-00000.json.gz", "k\xff\xfe?", "%41%", "\U0001F600*~"} {
		f.Add([]byte(s), 5)
	}
	f.Fuzz(func(t *testing.T, v []byte, chunk int) {
		chunk = 1 + (chunk&0x7fffffff)%64
		want := AppendPercentEncoded(nil, v)
		o, err := NewChunked(NewBudget(1<<30, nil), 1, 1<<30, chunk, nil)
		if err != nil {
			t.Fatal(err)
		}
		o.PercentEncoded(v)
		if got := body(o); !bytes.Equal(got, want) {
			t.Fatalf("chunk %d: %q, want %q", chunk, got, want)
		}
		o.Close()
		if back, ok := percentDecode(want); !ok || !bytes.Equal(back, v) {
			t.Fatalf("%q encoded %q decodes to %q (%v)", v, want, back, ok)
		}
	})
}

// percentDecode reads S3's url encoding back: '+' is a space and %XX with
// upper-case hex a byte; any other '%' or a byte the encoder never writes
// unescaped is a failure.
func percentDecode(s []byte) ([]byte, bool) {
	var out []byte
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '+':
			out = append(out, ' ')
		case c == '%':
			if i+2 >= len(s) {
				return nil, false
			}
			hi, lo := strings.IndexByte(hexDigits, s[i+1]), strings.IndexByte(hexDigits, s[i+2])
			if hi < 0 || lo < 0 {
				return nil, false
			}
			out = append(out, byte(hi<<4|lo))
			i += 2
		case urlPlain[c] == c:
			out = append(out, c)
		default:
			return nil, false
		}
	}
	return out, true
}
