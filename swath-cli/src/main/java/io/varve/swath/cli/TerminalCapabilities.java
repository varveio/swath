/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

/**
 * Independent per-fd terminal (isatty) detection — the confirmed JDK-25 fix.
 * {@code System.console() != null} conflates stdin AND stdout: it is {@code null} whenever
 * EITHER fd is non-tty, so {@code cat urls | swath list -o - > out} run <em>inside</em> a real
 * terminal (stdout IS a tty, stdin is piped) is wrongly classified non-terminal. This
 * probes fd 1 (stdout) and fd 2 (stderr) independently via {@code isatty(3)} (POSIX) / a console
 * handle (Windows), so {@code --format auto} and the stderr run-summary can each ask the fd they
 * actually care about.
 *
 * <p>Never throws: any probe failure (unsupported OS, missing symbol, sandboxed environment)
 * conservatively reports "not a terminal" rather than crashing the CLI.
 */
final class TerminalCapabilities {

    /** POSIX file descriptor numbers for the process's standard streams. */
    static final int STDOUT_FD = 1;
    static final int STDERR_FD = 2;

    /** Pluggable per-fd probe — the test seam (real terminals are not available under a build). */
    @FunctionalInterface
    interface FdProbe {
        boolean isTerminal(int fd);
    }

    private final FdProbe probe;

    TerminalCapabilities() {
        this(TerminalCapabilities::nativeIsTerminal);
    }

    /** Test-only constructor: inject a fake probe instead of the real native one. */
    TerminalCapabilities(FdProbe probe) {
        this.probe = probe;
    }

    boolean stdoutIsTerminal() {
        return probe.isTerminal(STDOUT_FD);
    }

    boolean stderrIsTerminal() {
        return probe.isTerminal(STDERR_FD);
    }

    private static boolean nativeIsTerminal(int fd) {
        try {
            String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
            return os.contains("win") ? WindowsConsole.isTerminal(fd) : PosixIsatty.isTerminal(fd);
        } catch (Throwable t) {
            return false;   // conservative: never let a probe failure crash the CLI
        }
    }

    /** {@code isatty(3)} via the Foreign Function & Memory API (stable since JDK 22 — no preview). */
    private static final class PosixIsatty {
        private static final MethodHandle ISATTY = bind();

        private static MethodHandle bind() {
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = linker.defaultLookup();
            return lookup.find("isatty")
                    .map(symbol -> linker.downcallHandle(symbol,
                            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)))
                    .orElse(null);
        }

        static boolean isTerminal(int fd) throws Throwable {
            if (ISATTY == null) {
                return false;
            }
            return (int) ISATTY.invokeExact(fd) != 0;
        }
    }

    /**
     * Windows console-handle probe: {@code GetStdHandle} the standard handle for {@code fd}, then
     * {@code GetConsoleMode} on it — it only succeeds on a real console, not a pipe/file redirect.
     */
    private static final class WindowsConsole {
        private static final int STD_OUTPUT_HANDLE = -11;
        private static final int STD_ERROR_HANDLE = -12;
        private static final MethodHandle GET_STD_HANDLE = bind("GetStdHandle",
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
        private static final MethodHandle GET_CONSOLE_MODE = bind("GetConsoleMode",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        private static MethodHandle bind(String name, FunctionDescriptor descriptor) {
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = SymbolLookup.libraryLookup("kernel32", Arena.global());
            return lookup.find(name)
                    .map(symbol -> linker.downcallHandle(symbol, descriptor))
                    .orElse(null);
        }

        static boolean isTerminal(int fd) throws Throwable {
            if (GET_STD_HANDLE == null || GET_CONSOLE_MODE == null) {
                return false;
            }
            long handle = (long) GET_STD_HANDLE.invokeExact(fd == STDOUT_FD ? STD_OUTPUT_HANDLE : STD_ERROR_HANDLE);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment mode = arena.allocate(ValueLayout.JAVA_INT);
                int ok = (int) GET_CONSOLE_MODE.invokeExact(handle, mode);
                return ok != 0;
            }
        }
    }
}
