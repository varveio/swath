/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.varve.swath.observability.StopReason;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * The graceful SIGTERM/SIGINT handler ({@link SignalHandlers}). A raised signal
 * flips the registered token's cancellation with {@link StopReason#SIGNAL} (the handler
 * does no summary/checkpoint work — that is the run thread's job on unwind). The raise is
 * guarded by {@link SignalHandlers#handlersActive()} so it never triggers the JVM's default
 * (fatal) action on a platform where the handler could not be installed.
 */
final class SignalHandlersTest {

    @Test
    void raisedTermCancelsTheRegisteredTokenWithSignalReason() {
        CancellationToken token = new CancellationToken();
        SignalHandlers.register(token);
        try {
            assumeTrue(SignalHandlers.handlersActive(), "SIGTERM handler not installable on this platform");
            Signal.raise(new Signal("TERM"));
            await().atMost(5, TimeUnit.SECONDS).until(token::isCancelled);
            assertThat(token.stopReason()).isEqualTo(StopReason.SIGNAL);
        } finally {
            SignalHandlers.unregister(token);
        }
    }

    @Test
    void unregisterStopsTargetingTheToken() {
        CancellationToken first = new CancellationToken();
        SignalHandlers.register(first);
        assumeTrue(SignalHandlers.handlersActive(), "SIGTERM handler not installable on this platform");
        SignalHandlers.unregister(first);
        // A signal handler is re-installed for the new active run (unregister restores the OS
        // disposition, so targeting a second token needs a fresh register — this also proves the
        // handler is *not* stuck permanently swallowing, i.e. it's usable again).
        CancellationToken second = new CancellationToken();
        SignalHandlers.register(second);
        try {
            Signal.raise(new Signal("TERM"));
            await().atMost(5, TimeUnit.SECONDS).until(second::isCancelled);
            assertThat(first.isCancelled()).isFalse();
        } finally {
            SignalHandlers.unregister(second);
        }
    }

    /**
     * {@code unregister} must restore whatever INT/TERM disposition preceded our
     * install (normally the JVM default, terminate) — a handler left in place with no active
     * token would silently swallow every SIGTERM/SIGINT for the rest of the process (e.g. the
     * remainder of a Gradle test fork's life after any test calls {@code ListCommand.call()}),
     * leaving it stoppable only by SIGKILL. We verify the *disposition*, not by raising the
     * signal — raising it after a real restore would hit the JVM's default terminate action and
     * kill this test JVM. {@link Signal#handle} atomically swaps the handler and returns the one
     * it replaced, so swapping in a harmless probe and reading what comes back tells us the
     * disposition without ever invoking it; we immediately put back what we found.
     */
    @Test
    void unregisterRestoresThePreExistingDisposition() {
        // Stand in for "whatever was installed before our run" (in real life: the JVM's own
        // java.lang.Terminator handler, or an embedder's own handler) with a known, harmless Java
        // lambda, so identity can be checked precisely. (A JDK-native disposition like
        // Terminator's is returned from sun.misc.Signal.handle wrapped in a fresh proxy object on
        // every read, so two separate reads of the *same* native disposition are unequal Java
        // objects — not a usable equality signal. A Java-supplied SignalHandler, by contrast, is
        // stored and returned by reference, so identity equality is exactly the right check.)
        SignalHandler baseline = raised -> { };
        SignalHandler original = Signal.handle(new Signal("TERM"), baseline);
        try {
            CancellationToken token = new CancellationToken();
            SignalHandlers.register(token);
            assumeTrue(SignalHandlers.handlersActive(), "SIGTERM handler not installable on this platform");
            SignalHandlers.unregister(token);

            // Probe without ever invoking anything: swapping in a throwaway handler and reading
            // back what unregister restored, without raising the signal.
            SignalHandler probe = raised -> { };
            SignalHandler restored = Signal.handle(new Signal("TERM"), probe);
            assertThat(restored).isSameAs(baseline);
        } finally {
            Signal.handle(new Signal("TERM"), original);   // put the true original back
        }
    }

    /**
     * {@code install()} must never leave a HALF-installed handler. If the INT swap succeeds but
     * the TERM swap then throws, {@code installed} stays false — so {@code unregister()}/{@code
     * restore()} would no-op — and our INT handler would silently swallow every SIGINT for the rest
     * of the JVM's life, unrestorable. The fix rolls INT back to its prior disposition on a partial
     * failure. We drive the failure through the package-private {@link SignalHandlers#registrar}
     * seam (a real TERM-install failure is impossible to induce deterministically on a live JVM) and
     * verify INT's disposition WITHOUT raising the signal (probe-and-read-back), so we never invoke a
     * restored JVM-default terminate action. Guarded by a real install probe for platform support.
     */
    @Test
    void partialInstallFailure_rollsBackIntToItsPriorDisposition() {
        // Platform guard: prove signal handlers are installable here (also leaves INT/TERM restored).
        CancellationToken probeToken = new CancellationToken();
        SignalHandlers.register(probeToken);
        boolean installable = SignalHandlers.handlersActive();
        SignalHandlers.unregister(probeToken);
        assumeTrue(installable, "signal handlers not installable on this platform");

        // Stand in for "whatever INT disposition preceded our run" with a known, harmless lambda so
        // identity can be checked precisely (see the sibling test's rationale).
        SignalHandler baselineInt = raised -> { };
        SignalHandler originalInt = Signal.handle(new Signal("INT"), baselineInt);

        // Precondition: this process must actually let us OWN SIGINT. When the JVM is launched with
        // SIGINT already set to SIG_IGN — a background/nohup process, or a test harness that ignores
        // SIGINT — HotSpot keeps INT ignored and refuses to install a Java handler, so the baseline
        // above never takes and this test's rollback assertion is meaningless (it would read back
        // SIG_IGN, not our lambda). Detect it by reading INT back; if our baseline didn't stick,
        // restore the original disposition and skip (mirrors the handlersActive() platform guard).
        // This is why the test runs+asserts in normal/foreground CI but self-skips under a
        // SIGINT-ignoring launcher (e.g. a backgrounded local Gradle run).
        SignalHandler intReadBack = Signal.handle(new Signal("INT"), baselineInt);
        boolean intSettable = intReadBack == baselineInt;
        if (!intSettable) {
            Signal.handle(new Signal("INT"), originalInt);
        }
        assumeTrue(intSettable, "SIGINT is SIG_IGN at JVM startup (background/nohup process or a "
                + "SIGINT-ignoring harness); cannot exercise INT-handler rollback here");

        SignalHandlers.SignalRegistrar realRegistrar = SignalHandlers.registrar;
        CancellationToken token = new CancellationToken();
        try {
            // Seam: INT swaps for real; the TERM swap throws AFTER INT already succeeded — the exact
            // partial-install shape.
            SignalHandlers.registrar = (sig, handler) -> {
                if ("TERM".equals(sig.getName())) {
                    throw new IllegalArgumentException("simulated TERM install failure");
                }
                return realRegistrar.handle(sig, handler);
            };
            SignalHandlers.register(token);   // INT swapped, TERM throws ⇒ INT must roll back

            // Restore the real primitive BEFORE probing so the read-back uses the true disposition.
            SignalHandlers.registrar = realRegistrar;
            SignalHandler probe = raised -> { };
            SignalHandler afterInt = Signal.handle(new Signal("INT"), probe);
            assertThat(afterInt)
                    .as("INT rolled back to its prior disposition after a partial (TERM) install failure")
                    .isSameAs(baselineInt);
        } finally {
            SignalHandlers.registrar = realRegistrar;
            SignalHandlers.unregister(token);
            Signal.handle(new Signal("INT"), originalInt);   // put the true original back
        }
    }
}
