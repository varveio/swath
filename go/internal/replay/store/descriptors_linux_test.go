// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/sortedparquet"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

const (
	descriptorChild = "SWATH_STORE_DESCRIPTOR_CHILD"
	descriptorLimit = "SWATH_STORE_DESCRIPTOR_LIMIT"
)

// TestOpenManyPartsWithinDescriptorBudget opens a fixture of 300 one-row
// parts in child processes whose soft descriptor limit is 128 and 64, with
// the default MaxOpenFiles, and pages it to the end from concurrent
// readers, sampling the process's open descriptors throughout Open and the
// reads. Before the cache every part held a descriptor from Open on, and
// Open failed with too many open files; before the bound fell below 64 and
// slots were reserved ahead of each open, the limit of 64 failed too.
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
	for _, limit := range []string{"128", "64"} {
		t.Run("limit-"+limit, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestOpenManyPartsWithinDescriptorBudget$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), descriptorChild+"="+dir, descriptorLimit+"="+limit)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			t.Logf("child:\n%s", out)
		})
	}
}

func descriptorChildRun(t *testing.T, dir string) {
	var soft uint64
	if _, err := fmt.Sscan(os.Getenv(descriptorLimit), &soft); err != nil {
		t.Fatal(err)
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = soft
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	bound := sortedparquet.DefaultMaxOpenFiles()
	if bound != int(soft/2) {
		t.Fatalf("default bound %d under a soft limit of %d", bound, soft)
	}
	// Sample the descriptors the process holds while it opens and reads.
	var peak atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			if fds, err := os.ReadDir("/proc/self/fd"); err == nil {
				peak.Store(max(peak.Load(), int64(len(fds))))
			}
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()
	baseline, _ := os.ReadDir("/proc/self/fd")
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
	close(stop)
	<-sampled
	st := s.(*Sorted).Files().Stats()
	// The sampler's own directory read holds one descriptor.
	t.Logf("soft limit %d, bound %d: cache %+v; %d descriptors before Open, at most %d during it and the reads",
		soft, bound, st, len(baseline), peak.Load())
	if st.Peak > bound || peak.Load() > int64(len(baseline)+bound+1) {
		t.Fatalf("cache peak %d, process peak %d descriptors over a baseline of %d", st.Peak, peak.Load(), len(baseline))
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

const fifoChild = "SWATH_STORE_FIFO_CHILD"

// TestReopenRefusesAFIFO replaces an evicted part's path with a FIFO that
// has no writer and reads the part again, in a child process with a
// deadline: the read fails at once (not a regular file) instead of
// blocking in open, and Close returns. Before, the reopen blocked in open
// until a writer appeared.
func TestReopenRefusesAFIFO(t *testing.T) {
	if dir := os.Getenv(fifoChild); dir != "" {
		fifoChildRun(t, dir)
		return
	}
	dir := t.TempDir()
	writePart(t, filepath.Join(dir, "part-00001.parquet"), sortedparquettest.Part{Rows: objects("a"), Stamps: sortedparquettest.Stamps(1, false)})
	writePart(t, filepath.Join(dir, "part-00002.parquet"), sortedparquettest.Part{Rows: objects("m"), Stamps: sortedparquettest.Stamps(2, true)})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReopenRefusesAFIFO$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), fifoChild+"="+dir)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the child did not finish within 20 s: a reopen blocked\n%s", out)
	}
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
}

func fifoChildRun(t *testing.T, dir string) {
	s, _, err := Open(context.Background(), dir, Options{MaxOpenFiles: 1})
	if err != nil {
		t.Fatal(err)
	}
	read := func(key string) error {
		_, err := s.Rows(context.Background(), []byte(key), true, nil, 1, Projection{})
		return err
	}
	if err := read("a"); err != nil {
		t.Fatal(err)
	}
	if err := read("m"); err != nil { // evicts part 1
		t.Fatal(err)
	}
	path := filepath.Join(dir, "part-00001.parquet")
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := read("a"); !errors.Is(err, sortedparquet.ErrNotRegular) {
		t.Fatalf("read of a part replaced by a FIFO: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
