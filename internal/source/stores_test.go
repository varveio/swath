// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import "testing"

func TestBackblaze(t *testing.T) {
	for ep, want := range map[string]bool{
		"https://s3.us-east-005.backblazeb2.com":        true,
		"https://s3.eu-central-003.backblazeb2.com/":    true,
		"https://bucket.s3.us-west-004.backblazeb2.com": true,
		"s3.us-west-002.backblazeb2.com":                true,
		"HTTPS://S3.US-EAST-005.BACKBLAZEB2.COM.":       true,
		"":                                    false,
		"https://backblazeb2.com.example.com": false,
		"https://s3.amazonaws.com":            false,
		"https://ACCOUNT.r2.cloudflarestorage.com":      false,
		"http://127.0.0.1:9000/?host=x.backblazeb2.com": false,
		"https://notbackblazeb2.com":                    false,
	} {
		if got := Backblaze(ep); got != want {
			t.Errorf("%q: %v, want %v", ep, got, want)
		}
	}
}
