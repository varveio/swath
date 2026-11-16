// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package longpath

import (
	"errors"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// deepDir builds base/n/n/... through directory handles, levels deep, with
// names of 200 bytes, a file f in the last, a symbolic link up to the
// level before it, and a link to f; it returns the last directory's path.
func deepDir(t *testing.T, base string, levels int) string {
	t.Helper()
	name := strings.Repeat("n", 200)
	dir, err := unix.Open(base, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	path := base
	for range levels {
		if err := unix.Mkdirat(dir, name, 0o755); err != nil {
			t.Fatal(err)
		}
		next, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		unix.Close(dir)
		if err != nil {
			t.Fatal(err)
		}
		dir, path = next, path+"/"+name
	}
	defer unix.Close(dir)
	fd, err := unix.Openat(dir, "f", unix.O_CREAT|unix.O_WRONLY|unix.O_CLOEXEC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	unix.Close(fd)
	if unix.Symlinkat("..", dir, "up") != nil || unix.Symlinkat("f", dir, "lf") != nil {
		t.Fatal("symlink")
	}
	return path
}

// TestLongPaths: Stat and Open resolve a path past PATH_MAX as the kernel
// resolves a short one: symbolic links followed, ".." the parent; a path
// that fits takes the plain call, and an error keeps its cause.
func TestLongPaths(t *testing.T) {
	base := t.TempDir()
	dir := deepDir(t, base, 30)
	if _, err := os.Stat(dir); !TooLong(err) {
		t.Fatalf("fixture: %d bytes, %v", len(dir), err)
	}
	fi, err := Stat(dir + "/f")
	if err != nil || fi.IsDir() || fi.Name() != "f" {
		t.Fatalf("f: %v %v", fi, err)
	}
	if lf, err := Stat(dir + "/lf"); err != nil || !os.SameFile(lf, fi) {
		t.Fatalf("a link to f: %v %v", lf, err)
	}
	parent := dir[:strings.LastIndexByte(dir, '/')]
	pfi, err := Stat(parent)
	if err != nil || !pfi.IsDir() {
		t.Fatal(err)
	}
	for _, p := range []string{dir + "/up", dir + "/..", dir + "/./up/"} {
		if got, err := Stat(p); err != nil || !os.SameFile(got, pfi) {
			t.Fatalf("%s: %v %v", p[len(dir):], got, err)
		}
	}
	f, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil || len(names) != 3 {
		t.Fatalf("%q %v", names, err)
	}
	for _, p := range []string{dir + "/missing", dir + "/" + strings.Repeat("y", 300)} {
		if _, err := Stat(p); err == nil || TooLong(err) != strings.HasSuffix(p, "y") || errors.Is(err, os.ErrNotExist) == strings.HasSuffix(p, "y") {
			t.Fatalf("%s: %v", p[len(dir):], err)
		}
	}
	if fi, err := Stat(base); err != nil || !fi.IsDir() {
		t.Fatalf("a short path: %v", err)
	}
}
