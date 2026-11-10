// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var fastTimeCases = []string{
	"2024-05-17T12:34:56Z",
	"2024-05-17T12:34:56.1Z",
	"2024-05-17T12:34:56.123Z",
	"2024-05-17T12:34:56.123456Z",
	"2024-05-17T12:34:56.123456789Z",
	"2024-05-17T12:34:56.1234567890Z",
	"2024-05-17T12:34:56.Z",
	"1970-01-01T00:00:00Z",
	"1969-12-31T23:59:59.999999999Z",
	"2024-02-29T00:00:00Z",
	"2000-02-29T00:00:00Z",
	"1900-02-29T00:00:00Z",
	"2023-02-29T00:00:00Z",
	"2024-01-00T00:00:00Z",
	"2024-01-32T00:00:00Z",
	"2024-00-10T00:00:00Z",
	"2024-13-10T00:00:00Z",
	"2024-04-31T00:00:00Z",
	"2024-05-17T24:00:00Z",
	"2024-05-17T23:60:00Z",
	"2024-05-17T23:59:60Z",
	"0001-01-01T00:00:00Z",
	"9999-12-31T23:59:59.999999999Z",
	"0000-01-01T00:00:00Z",
	"2024-05-17T12:34:56+00:00",
	"2024-05-17T12:34:56+05:30",
	"2024-05-17T12:34:56.5-08:00",
	"2024-05-17T12:34:56",
	"2024-05-17 12:34:56Z",
	"2024-05-17T12:34:5Z",
	"2024-05-17T12:34Z",
	"2024-05-17T12:34:56ZZ",
	"2024-05-17T12:34:56Z ",
	" 2024-05-17T12:34:56Z",
	"2024-05-17T12:34:56.12a4Z",
	"2024-05-17T12:34:56,123Z",
	"a024-05-17T12:34:56Z",
	"2024-a5-17T12:34:56Z",
	"2024-05-a7T12:34:56Z",
	"2024-05-17Ta2:34:56Z",
	"2024-05-17T12:a4:56Z",
	"2024-05-17T12:34:a6Z",
	"2024-05-17T12:34:56.1a3Z",
	"+024-05-17T12:34:56Z",
	"2024-05-17T12:34:56z",
	"",
	"Z",
}

// checkFastTime holds fastTime to time.Time.UnmarshalText: an accepted input
// has the same instant, and a declined one is decided by the fallback.
func checkFastTime(t *testing.T, in string) {
	t.Helper()
	var std time.Time
	stdErr := std.UnmarshalText([]byte(in))
	if us, ok := fastTime([]byte(in)); ok {
		if stdErr != nil {
			t.Fatalf("%q: fast path accepted, standard parser rejected: %v", in, stdErr)
		}
		if want := std.UnixMicro(); us != want {
			t.Fatalf("%q: fast %d, standard %d", in, us, want)
		}
	}
	if in == "" {
		return // parseTime maps empty to 0
	}
	got, err := parseTime([]byte(in))
	if (err != nil) != (stdErr != nil) {
		t.Fatalf("%q: parseTime err %v, standard err %v", in, err, stdErr)
	}
	if err == nil && got != std.UnixMicro() {
		t.Fatalf("%q: parseTime %d, standard %d", in, got, std.UnixMicro())
	}
}

func TestFastTimeMatchesStandard(t *testing.T) {
	for _, in := range fastTimeCases {
		checkFastTime(t, in)
	}
	checkFastTime(t, "2024-05-17T12:34:56."+strings.Repeat("1", 64)+"Z")
	for m := 0; m <= 13; m++ {
		for d := 0; d <= 32; d++ {
			checkFastTime(t, fmt.Sprintf("2024-%02d-%02dT00:00:00Z", m, d))
			checkFastTime(t, fmt.Sprintf("1900-%02d-%02dT00:00:00Z", m, d))
		}
	}
	for y := 0; y <= 9999; y += 4 {
		checkFastTime(t, fmt.Sprintf("%04d-02-29T00:00:00Z", y))
	}
	full := "2024-05-17T12:34:56.123456Z"
	for i := range full {
		checkFastTime(t, full[:i])
	}
}

func FuzzFastTime(f *testing.F) {
	for _, in := range fastTimeCases {
		f.Add(in)
	}
	f.Fuzz(func(t *testing.T, in string) {
		checkFastTime(t, in)
	})
}

func BenchmarkFastTime(b *testing.B) {
	const in = "2024-05-17T12:34:56.123Z"
	b.Run("fast", func(b *testing.B) {
		p := []byte(in)
		for range b.N {
			if _, ok := fastTime(p); !ok {
				b.Fatal("declined")
			}
		}
	})
	b.Run("UnmarshalText", func(b *testing.B) {
		p := []byte(in)
		for range b.N {
			var tm time.Time
			if err := tm.UnmarshalText(p); err != nil {
				b.Fatal(err)
			}
			_ = tm.UnixMicro()
		}
	})
}
