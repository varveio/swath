// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"encoding/xml"
	"runtime"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
)

// TestOversizedContentTypeStaysBounded renders a GCS recording's row whose
// content type is 1 MiB of '&' (each escaped to five bytes) under an 8 KiB
// cap and an 8 KiB budget: the render stops at the cap, never charges past
// it, and allocates the staging buffer and the capped output, not the
// value's rendering. Escaping the value into the staged entry before
// writing it allocated over 5 MiB uncharged.
func TestOversizedContentTypeStaysBounded(t *testing.T) {
	big := strings.Repeat("&", 1<<20)
	res := &Result{Request: parse(t, ""), Entries: []Entry{{Object: &store.Object{Key: []byte("a"), ContentType: &big}}}}
	budget := output.NewBudget(8192, nil)
	var o *output.Output
	var err error
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	o, err = output.New(budget, 4096, 8192)
	if err == nil {
		err = RenderList(o, res, "http://h:1/replay/")
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if err != nil {
		t.Fatal(err)
	}
	if o.Err() != output.ErrTooLarge {
		t.Errorf("render: %v", o.Err())
	}
	if budget.Peak() > 8192 {
		t.Errorf("peak charge %d over the cap", budget.Peak())
	}
	if allocated > 64<<10 {
		t.Errorf("rendering allocated %d bytes for a %d-byte content type under an 8 KiB cap", allocated, len(big))
	}
	o.Close()
	if budget.Charged() != 0 {
		t.Errorf("%d still charged", budget.Charged())
	}
	t.Logf("allocated %d bytes", allocated)
}

// TestContentTypeOutsideXMLTakesThePlaceholder renders recorded content
// types holding U+FFFE or U+FFFF, valid UTF-8 that XML 1.0's Char
// production excludes (the surrogates it also excludes cannot occur in
// valid UTF-8), and requires the body to parse with encoding/xml and carry
// the placeholder; the neighbours XML allows (U+FFFD, U+10000, U+E000,
// U+D7FF) are kept. Before, the two were written as they are and the body
// did not parse.
func TestContentTypeOutsideXMLTakesThePlaceholder(t *testing.T) {
	for _, c := range []struct{ ct, want string }{
		{"text/\ufffe", placeholderContentType},
		{"text/\uffff", placeholderContentType},
		{"\uffff", placeholderContentType},
		{"text/plain; x=\ufffd\U00010000\ue000\ud7ff", "text/plain; x=\ufffd\U00010000\ue000\ud7ff"},
	} {
		ct := c.ct
		res := &Result{Request: parse(t, ""), Entries: []Entry{{Object: &store.Object{Key: []byte("a"), ContentType: &ct}}}}
		body, err := AppendList(nil, res, "http://h:1/replay/")
		if err != nil {
			t.Fatalf("%+q: %v", c.ct, err)
		}
		var doc struct {
			Blobs []struct {
				ContentType string `xml:"Properties>Content-Type"`
			} `xml:"Blobs>Blob"`
		}
		if err := xml.Unmarshal([]byte(strings.TrimPrefix(string(body), BOM)), &doc); err != nil {
			t.Errorf("%+q: the body does not parse: %v", c.ct, err)
			continue
		}
		if len(doc.Blobs) != 1 || doc.Blobs[0].ContentType != c.want {
			t.Errorf("%+q: %+q, want Content-Type %+q", c.ct, doc.Blobs, c.want)
		}
	}
}
