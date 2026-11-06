// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/textout"
)

// TestS3Line: an S3 object's JSONL line is, byte for byte, what
// `aws s3api list-objects-v2 | jq -c '.Contents[]'` prints for it: the
// API's field order, the ETag with its quotes (the provider stores it
// bare), the checksum algorithm as a list, LastModified as the CLI
// prints it; Owner only when fetched.
func TestS3Line(t *testing.T) {
	var buf bytes.Buffer
	o, err := textout.Open(textout.Config{Format: textout.JSONL, Stdout: &buf})
	if err != nil {
		t.Fatal(err)
	}
	k, err := textout.NewSink(o, &s3.Schema)
	if err != nil {
		t.Fatal(err)
	}
	segs := [][]s3.Row{{
		{Key: "logs/2023/app.log", Data: s3.Object{Size: 434234, Mtime: 1_700_000_000_000_000, ETag: "fba9dede5f27731c9771645a39863328",
			StorageClass: "STANDARD", ChecksumAlgorithm: "CRC64NVME", ChecksumType: "FULL_OBJECT"}},
		{Key: "big.bin", Data: s3.Object{Size: 5, Mtime: 1_700_000_000_000_000, ETag: "d41d8cd98f00b204e9800998ecf8427e-2",
			StorageClass: "GLACIER", OwnerID: "abc"}},
	}}
	if err := k.Write(context.Background(), segs); err != nil {
		t.Fatal(err)
	}
	if err := o.Commit(); err != nil {
		t.Fatal(err)
	}
	want := `{"Key":"logs/2023/app.log","LastModified":"2023-11-14T22:13:20+00:00","ETag":"\"fba9dede5f27731c9771645a39863328\"","ChecksumAlgorithm":["CRC64NVME"],"ChecksumType":"FULL_OBJECT","Size":434234,"StorageClass":"STANDARD"}` + "\n" +
		`{"Key":"big.bin","LastModified":"2023-11-14T22:13:20+00:00","ETag":"\"d41d8cd98f00b204e9800998ecf8427e-2\"","Size":5,"StorageClass":"GLACIER","Owner":{"ID":"abc"}}` + "\n"
	if got := buf.String(); got != want {
		t.Fatalf("jsonl\n%s\nwant\n%s", got, want)
	}
}
