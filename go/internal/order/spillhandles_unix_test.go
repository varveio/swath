// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package order

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
)

// TestHandlesLowFDLimit: under a descriptor limit the process has used up,
// with ten idle handles open, a read of another file still succeeds: its
// open fails (EMFILE), the idle handles are closed and the open retried.
func TestHandlesLowFDLimit(t *testing.T) {
	s, err := newSpiller(&testSchema, t.TempDir(), false, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	var files []*spillFile
	var ats [][]spillAt
	for i := range 11 {
		f, at := handleFile(t, s, fmt.Sprintf("l%02d", i), 1, 4)
		files, ats = append(files, f), append(ats, at)
	}
	for i := range 10 {
		if _, _, err := s.readAt(files[i], ats[i][0].off, ats[i][0].size); err != nil {
			t.Fatal(err)
		}
	}
	ents, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Skip(err)
	}
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		t.Skip(err)
	}
	low := lim
	low.Cur = uint64(len(ents) + 16)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &low); err != nil {
		t.Skip(err)
	}
	var fill []*os.File
	defer func() {
		for _, f := range fill {
			f.Close()
		}
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
			t.Error(err)
		}
	}()
	for {
		f, err := os.Open(os.DevNull)
		if errors.Is(err, syscall.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		fill = append(fill, f)
		if len(fill) > 1<<12 {
			t.Skip("the descriptor limit did not bind")
		}
	}
	rows, _, err := s.readAt(files[10], ats[10][0].off, ats[10][0].size)
	if err != nil {
		t.Fatalf("read at the descriptor limit: %v", err)
	}
	sameRows(t, "at the limit", rows, handleRows("l10", 0, 4))
	if st := s.h.stats(); st.Evictions.LowFD != 10 || s.h.openFDs() != 0 {
		t.Fatalf("%+v, %d open", st, s.h.openFDs())
	}
}
