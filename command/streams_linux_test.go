// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// TestMainStreamsDecideTerminal: terminal detection follows the stdout
// Main is given, not the process's. A child whose own stdout is a terminal
// (a pseudo-terminal here) calls Main with a buffer: --format auto writes
// the rows a pipe gets from the binary (TSV), not the table.
func TestMainStreamsDecideTerminal(t *testing.T) {
	ptmx, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("no pseudo-terminal: %v", err)
	}
	defer ptmx.Close()
	if err := unix.IoctlSetPointerInt(int(ptmx.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Skipf("unlock pseudo-terminal: %v", err)
	}
	n, err := unix.IoctlGetInt(int(ptmx.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Skipf("pseudo-terminal number: %v", err)
	}
	pts, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("open pseudo-terminal: %v", err)
	}

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMainStreamsProcess$")
	cmd.Env = append(os.Environ(), "SWATH_TEST_STREAMS="+src)
	cmd.Stdout = pts
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pts.Close() // the child holds it; the master reads EIO once the child exits
	var tty bytes.Buffer
	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(&tty, ptmx)
		close(copied)
	}()
	err = cmd.Wait()
	ptmx.Close()
	<-copied
	if err != nil {
		t.Fatalf("child: %v\nterminal: %s\nstderr: %s", err, tty.String(), stderr.String())
	}
}

// TestMainStreamsProcess is run only by TestMainStreamsDecideTerminal's
// child, whose stdout is a terminal.
func TestMainStreamsProcess(t *testing.T) {
	src := os.Getenv("SWATH_TEST_STREAMS")
	if src == "" {
		t.Skip("subprocess helper for TestMainStreamsDecideTerminal")
	}
	if !term.IsTerminal(int(os.Stdout.Fd())) {
		t.Fatal("the child's stdout is not a terminal")
	}
	args := []string{"list", src} // --columns would choose TSV itself
	var auto, tsv bytes.Buffer
	if code := Main(context.Background(), args, nil, &auto, io.Discard); code != exitOK {
		t.Fatalf("auto: exit %d", code)
	}
	if code := Main(context.Background(), append(args, "--format", "tsv"), nil, &tsv, io.Discard); code != exitOK {
		t.Fatalf("tsv: exit %d", code)
	}
	if auto.String() != tsv.String() {
		t.Fatalf("--format auto into a buffer:\n%s\nwant the TSV a pipe gets:\n%s", auto.String(), tsv.String())
	}
}
