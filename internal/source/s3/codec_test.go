// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/varveio/swath/internal/schema"
)

// TestDeltaCodecForms: the delta encoding picks the raw ETag only for
// exactly 32 lowercase hex digits (the spelling it gives back), the string
// for every other spelling; repeats cost a flag; an overflowing time
// difference falls back to the time itself; every form decodes to the
// same Object, owning its strings; an unknown ETag form is malformed.
func TestDeltaCodecForms(t *testing.T) {
	const md5 = "d41d8cd98f00b204e9800998ecf8427e"
	prev := Object{Mtime: math.MinInt64, ETag: "x", StorageClass: "STANDARD", ChecksumType: "FULL_OBJECT"}
	for _, c := range []struct {
		o    Object
		form byte
	}{
		{Object{Size: 5, Mtime: math.MaxInt64, ETag: md5, StorageClass: "STANDARD"}, etagRaw},
		{Object{ETag: "D41D8CD98F00B204E9800998ECF8427E"}, etagString},
		{Object{ETag: md5 + "-12"}, etagString},
		{Object{ETag: `"` + md5 + `"`}, etagString},
		{Object{ETag: md5[:31] + "g"}, etagString},
		{Object{ETag: ""}, etagString},
		{Object{ETag: "x", Size: -1, Mtime: -5}, etagSame},
	} {
		b := objectCodec{}.AppendDelta(nil, &prev, &c.o)
		if form := (b[0] & deltaETagMask) >> deltaETagShift; form != c.form {
			t.Fatalf("%q: form %d, want %d", c.o.ETag, form, c.form)
		}
		diff := new(big.Int).Sub(big.NewInt(c.o.Mtime), big.NewInt(prev.Mtime))
		if abs := b[0]&deltaTimeAbs != 0; abs != !diff.IsInt64() {
			t.Fatalf("%d after %d: absolute %v", c.o.Mtime, prev.Mtime, abs)
		}
		src := append([]byte(nil), b...)
		d := schema.NewOwningDecoder(src, 1<<10)
		var got Object
		objectCodec{}.DecodeDelta(d, &prev, &got)
		for i := range src {
			src[i] = 0 // the decoded Object must not view the frame
		}
		if d.Err() != nil || d.Len() != 0 || got != c.o {
			t.Fatalf("%+v decoded as %+v (%v)", c.o, got, d.Err())
		}
	}
	raw := objectCodec{}.AppendDelta(nil, &Object{}, &Object{ETag: md5, StorageClass: "STANDARD"})
	if len(raw) > 1+1+1+16+9 {
		t.Fatalf("a raw ETag row takes %d bytes", len(raw))
	}
	same := objectCodec{}.AppendDelta(nil, &Object{ETag: md5, StorageClass: "STANDARD", Mtime: 7}, &Object{ETag: md5, StorageClass: "STANDARD", Mtime: 8})
	if len(same) != 3 {
		t.Fatalf("a repeated row takes %d bytes, want 3 (flags, size, time)", len(same))
	}
	bad := objectCodec{}.AppendDelta(nil, &Object{}, &Object{})
	bad[0] |= deltaETagMask // form 3: unknown
	d := schema.NewOwningDecoder(bad, 1<<10)
	objectCodec{}.DecodeDelta(d, &Object{}, &Object{})
	if !errors.Is(d.Err(), schema.ErrCodec) {
		t.Fatalf("an unknown ETag form: %v", d.Err())
	}
	// A raw ETag decodes into owned storage, charged to the budget.
	d = schema.NewOwningDecoder(raw, 31)
	objectCodec{}.DecodeDelta(d, &Object{}, &Object{})
	if !errors.Is(d.Err(), schema.ErrDecodeCap) {
		t.Fatalf("a raw ETag past the budget: %v", d.Err())
	}
}
