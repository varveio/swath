// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/varveio/swath/go/sortedparquet"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

const descriptorChild = "SWATH_STORE_DESCRIPTOR_CHILD"

// TestOpenManyPartsWithinDescriptorBudget opens a fixture of 300 one-row
// parts in a child process whose soft descriptor limit is 128, with the
// default MaxOpenFiles, and pages it to the end from concurrent readers.
// Before the cache every part held a descriptor from Open on, and Open
// failed with too many open files.
func TestOpenManyPartsWithinDescriptorBudget(t *testing.T) {
	if dir := os.Getenv(descriptorChild); dir != "" {
		descriptorChildRun(t, dir)
		return
	}
	dir := t.TempDir()
	const parts = 300
	for i := range parts {
		part := sortedparquettest.Part{Rows: objects(fmt.Sprintf("k%04d", i)), Stamps: sortedparquettest.Stamps(i+1, i == parts-1)}
		writePart(t, filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", i+1)), part)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestOpenManyPartsWithinDescriptorBudget$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), descriptorChild+"="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	t.Logf("child:\n%s", out)
}

func descriptorChildRun(t *testing.T, dir string) {
	limit := syscall.Rlimit{Cur: 128, Max: 128}
	var current syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &current); err != nil {
		t.Fatal(err)
	}
	limit.Max = current.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	if got := sortedparquet.DefaultMaxOpenFiles(); got != 64 {
		t.Fatalf("default bound %d under a soft limit of 128, want 64", got)
	}
	s, meta, err := Open(context.Background(), dir, Options{Readers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if meta.Parts != 300 {
		t.Fatalf("%d parts", meta.Parts)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if n, err := walkQuiet(s); err != nil || n != 300 {
				t.Errorf("walk %d rows: %v", n, err)
			}
		})
	}
	wg.Wait()
	st := s.(*Sorted).Files().Stats()
	fds, _ := os.ReadDir("/proc/self/fd")
	t.Logf("cache %+v; %d descriptors open in the process", st, len(fds))
	if st.Peak > 64 || len(fds) > 64+32 {
		t.Fatalf("cache peak %d, %d descriptors open", st.Peak, len(fds))
	}
}

// walkQuiet pages s from the start at limit 7 and counts the rows; it
// reports an error instead of failing, for use off the test goroutine.
func walkQuiet(s Store) (int, error) {
	n := 0
	var from []byte
	for {
		page, err := s.Rows(context.Background(), from, false, nil, 7, Projection{})
		if err != nil {
			return -1, err
		}
		if len(page) == 0 {
			return n, nil
		}
		n += len(page)
		from = page[len(page)-1].Key
	}
}
