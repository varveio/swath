// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"encoding/xml"
	"fmt"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/utf16order"
)

// bom starts every body Azure sends, success or error (both service
// versions; every capture of the October 2026 campaign).
var bom = []byte("\xef\xbb\xbf")

// MaxPageSize is the most entries Azure returns on one page: a larger
// maxresults is answered with 5,000 (capture azure-08e) and an absent one
// means 5,000.
const MaxPageSize = 5000

// Validate holds a page to the listing contract: names (blobs and
// prefixes together, in wire order) ascend strictly in UTF-16 code-unit
// order, and the next marker is not the one sent (the listing would loop).
// An empty NextMarker is the end.
func (p *ListOutput) Validate(sent string) error {
	for i := 1; i < len(p.Items); i++ {
		if a, b := p.Items[i-1].Name, p.Items[i].Name; utf16order.Compare(a, b) >= 0 {
			return fmt.Errorf("%w: %q listed after %q (UTF-16 code-unit order)", objhttp.ErrInvalidPage, b, a)
		}
	}
	if p.NextMarker != "" && p.NextMarker == sent {
		return fmt.Errorf("%w: NextMarker repeats the marker sent", objhttp.ErrInvalidPage)
	}
	return nil
}

// errorMessage reads an <Error> document's Message (the BOM stripped) from
// the bounded body; empty when the body is not one.
func errorMessage(body []byte) string {
	var w struct {
		Message string `xml:"Message"`
	}
	if xml.Unmarshal(bytes.TrimPrefix(body, bom), &w) != nil {
		return ""
	}
	return w.Message
}

// errorCode preserves XML character decoding for headerless answers:
// azcore's regular expression does not read CDATA or entity references.
// The Guard has already bounded body to maxErrorBody.
func errorCode(body []byte) string {
	var w struct {
		Code string `xml:"Code"`
	}
	if xml.Unmarshal(bytes.TrimPrefix(body, bom), &w) != nil {
		return ""
	}
	return w.Code
}
