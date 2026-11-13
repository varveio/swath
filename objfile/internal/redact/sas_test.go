// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"net/url"
	"strings"
	"testing"
)

// TestSAS: a SAS's signature, with characters a URL reserves, is removed
// however the text spells it: as sent, decoded, escaped in lower case,
// escaped once more, or as a second sig value; the rest of the text stays.
func TestSAS(t *testing.T) {
	const secret = "ZQXSECRET"
	sas := "sv=2026-06-06&sp=rwd&sig=" + url.QueryEscape("AB+"+secret+"/CD=")
	scrub := SAS("?" + sas)
	query := "comp=block&" + sas + "&blockid=x"
	for _, text := range []string{
		query,
		"sig=AB+" + secret + "/CD=",
		strings.NewReplacer("%2B", "%2b", "%2F", "%2f", "%3D", "%3d").Replace(query),
		url.QueryEscape(query),
		"<Message>sig=AB%2b" + secret + "%2fCD%3d</Message>",
		"sv=1&sig=other&sig=AB%2b" + secret + "x",
	} {
		got := scrub("PUT /c/b?" + text + " failed")
		if strings.Contains(got, secret) || !strings.HasPrefix(got, "PUT /c/b?") || !strings.HasSuffix(got, " failed") {
			t.Errorf("%q: %q", text, got)
		}
	}
	if got := SAS("")("nothing secret here"); got != "nothing secret here" {
		t.Errorf("clean text changed: %q", got)
	}
}
