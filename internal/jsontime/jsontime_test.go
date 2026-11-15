// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package jsontime

import (
	"encoding/json"
	"testing"
	"time"
)

// TestFormat: six fraction digits whatever the time's precision, in UTC.
func TestFormat(t *testing.T) {
	east := time.FixedZone("east", 3*3600)
	for _, c := range []struct {
		t    time.Time
		want string
	}{
		{time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC), "2026-11-15T12:00:00.000000Z"},
		{time.Date(2026, 11, 15, 12, 0, 0, 137198400, time.UTC), "2026-11-15T12:00:00.137198Z"},
		{time.Date(2026, 11, 15, 12, 0, 0, 132533409, time.UTC), "2026-11-15T12:00:00.132533Z"},
		{time.Date(2026, 11, 15, 15, 0, 0, 1000, east), "2026-11-15T12:00:00.000001Z"},
	} {
		if got := Format(c.t); got != c.want {
			t.Errorf("Format(%v) = %s, want %s", c.t, got, c.want)
		}
		b, err := json.Marshal(struct {
			At  Time  `json:"at"`
			Opt *Time `json:"opt,omitempty"`
			Z   Time  `json:"z,omitzero"`
		}{At: Of(c.t)})
		if err != nil {
			t.Fatal(err)
		}
		if want := `{"at":"` + c.want + `"}`; string(b) != want {
			t.Errorf("marshalled %s, want %s", b, want)
		}
		var back struct {
			At Time `json:"at"`
		}
		if err := json.Unmarshal(b, &back); err != nil || !back.At.Equal(c.t.Truncate(time.Microsecond)) {
			t.Errorf("read back %v (%v), want %v", back.At, err, c.t.Truncate(time.Microsecond))
		}
	}
}
