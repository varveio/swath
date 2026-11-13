// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/order"
)

// TestOneFilesystem: with OneFilesystem, a directory on another device
// than the root is listed (its DIR row, unless NoDirs) but not walked, in
// both engines, and counted; on the root's device the listing is the whole
// tree, a chunked directory included. The other device is simulated by
// giving the provider another root device: every directory is then a
// mount point.
func TestOneFilesystem(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "top", false)
	mk(t, root, "d1/f", false)
	mk(t, root, "d1/sub/g", false)
	mk(t, root, "d2/h", false)
	for i := range 5000 { // chunked by the native engine
		mk(t, root, "big/f"+strconv.Itoa(i), false)
	}
	mk(t, root, "big/zz/inner", false)
	if _, err := New(root, Options{OneFilesystem: true, Fields: Names}); err == nil || !strings.Contains(err.Error(), "names metadata tier") {
		t.Fatalf("names tier: %v", err)
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		// The portable engine reads no device on Windows.
		if _, err := New(root, Options{OneFilesystem: true}); err == nil {
			t.Fatal("no device, yet one filesystem")
		}
		return
	}
	for _, portable := range []bool{false, true} {
		for _, noDirs := range []bool{false, true} {
			for _, other := range []bool{false, true} {
				t.Run(fmt.Sprintf("portable=%v/nodirs=%v/other=%v", portable, noDirs, other), func(t *testing.T) {
					p, err := New(root, Options{OneFilesystem: true, NoDirs: noDirs, Portable: portable})
					if err != nil {
						t.Fatal(err)
					}
					if other {
						p.dev++
					}
					c := &collect{}
					if _, err := order.Run[Range, Entry](context.Background(), p, c, &Schema, order.Options{Workers: 4, Budget: 1 << 20}); err != nil {
						t.Fatal(err)
					}
					var keys []string
					for _, r := range c.rows {
						keys = append(keys, r.Key)
					}
					want := oracle(t, root, !noDirs)
					wantOther := int64(0)
					if other {
						want = []string{"big/", "d1/", "d2/", "top"}
						if noDirs {
							want = []string{"top"}
						}
						wantOther = 3
					}
					if !slices.Equal(keys, want) {
						t.Fatalf("%d keys %.10q, want %d %.10q", len(keys), keys, len(want), want)
					}
					if st := p.Stats(); st.OtherDevice != wantOther {
						t.Fatalf("other_device_dirs %d, want %d", st.OtherDevice, wantOther)
					}
				})
			}
		}
	}
}
