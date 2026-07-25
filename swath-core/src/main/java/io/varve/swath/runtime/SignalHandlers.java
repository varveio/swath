/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.varve.swath.observability.StopReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Installs graceful SIGTERM/SIGINT handlers that request a {@link StopReason#SIGNAL}
 * cancellation of the run in progress. The main run thread observes the cancel,
 * unwinds through the pipeline's {@code try/finally}, writes the terminal JSON
 * summary + commits the resumable checkpoint, and the process then exits with the
 * mapped code (SIGINT → 130, SIGTERM → 143, told apart by the {@link CancelSource}
 * the handler attributes). The handler itself does <b>no</b> summary/checkpoint work
 * — it only flips the cancellation flag.
 *
 * <p><b>Why {@code sun.misc.Signal} and not a shutdown hook.</b> Installing a
 * {@code Signal.handle} for INT/TERM <i>replaces</i> the JVM's default action
 * (terminate). The JVM therefore does not begin its own shutdown on the signal —
 * the main thread stays alive to finish cleanup and drive a deliberate exit. That
 * sidesteps the shutdown-hook hazard entirely: with a hook the JVM is already
 * tearing down and the run races the exit to flush the summary/checkpoint (needing
 * a latch to block the hook until cleanup finishes). {@code Signal.handle} has no
 * such race. {@code sun.misc.Signal} lives in the {@code jdk.unsupported} module,
 * which is resolved and exported by default on JDK&nbsp;25 — no {@code --add-exports}
 * or {@code --enable-preview}.
 *
 * <p><b>Disposition is scoped to an active run, not the process.</b> {@link
 * #register(CancellationToken)} installs the handler (capturing whatever disposition
 * was previously in effect for INT/TERM — normally the JVM default, terminate) and
 * {@link #unregister(CancellationToken)} <i>restores</i> that captured disposition the
 * moment no run is targeted. This matters because a handler left permanently in place
 * with {@code active == null} would silently swallow every SIGTERM/SIGINT outside a
 * run — including for the entire remaining lifetime of a JVM that only ever ran one
 * command via {@link io.varve.swath.cli.ListCommand#call()} and returned (e.g. a Gradle
 * test fork), leaving it killable only by SIGKILL. Restoring on unregister guarantees
 * the invariant: outside an active run, SIGTERM/SIGINT terminate the JVM as normal.
 */
public final class SignalHandlers {

    private static final Logger log = LoggerFactory.getLogger(SignalHandlers.class);
    private static final Object LOCK = new Object();

    /**
     * The primitive that swaps a signal's disposition. Production uses {@link Signal#handle}; a
     * same-package test overrides it (guarded by {@link #LOCK}, restored in a finally) to simulate a
     * partial-install failure — a {@code TERM} swap that throws after {@code INT} already succeeded —
     * which is otherwise impossible to induce deterministically on a live JVM.
     */
    @FunctionalInterface
    interface SignalRegistrar {
        SignalHandler handle(Signal signal, SignalHandler handler);
    }

    static volatile SignalRegistrar registrar = Signal::handle;

    /** Sticky once true: the platform accepted our handler at least once (never reset). */
    private static volatile boolean handlersActive = false;

    /** Whether our handler currently occupies the INT/TERM disposition (guarded by {@link #LOCK}). */
    private static boolean installed = false;
    private static SignalHandler previousInt;
    private static SignalHandler previousTerm;
    private static volatile CancellationToken active;

    private SignalHandlers() {
    }

    /** Make {@code token} the target of SIGTERM/SIGINT, installing the OS handlers on first use. */
    public static void register(CancellationToken token) {
        synchronized (LOCK) {
            active = token;
            install();
        }
    }

    /**
     * Stop targeting {@code token} (only if it is still the active one) and — since no run is
     * targeted anymore — restore whatever INT/TERM disposition preceded our install, so the JVM
     * default (or an enclosing embedder's own handler) governs again outside a run.
     */
    public static void unregister(CancellationToken token) {
        synchronized (LOCK) {
            if (active != token) {
                return;
            }
            active = null;
            restore();
        }
    }

    /** True once the OS SIGINT/SIGTERM handlers were successfully installed (false on an unsupported platform). */
    public static boolean handlersActive() {
        return handlersActive;
    }

    private static void install() {
        if (installed) {
            return;
        }
        SignalHandler prevInt = null;
        boolean intSwapped = false;
        try {
            prevInt = registrar.handle(new Signal("INT"), SignalHandlers::onSignal);
            intSwapped = true;
            previousTerm = registrar.handle(new Signal("TERM"), SignalHandlers::onSignal);
            previousInt = prevInt;
            installed = true;
            handlersActive = true;
        } catch (RuntimeException e) {
            // Never leave a HALF-installed handler. If the INT swap already succeeded when the
            // TERM swap failed, roll INT back to its prior disposition — otherwise `installed` stays
            // false (so restore() no-ops) and our INT handler would silently swallow every SIGINT for
            // the JVM's lifetime, unrestorable by unregister(). Either fully installed or fully
            // rolled back. A platform without these signals (or a JVM that already reserved them)
            // simply falls back to the default action — the timebox path still works.
            if (intSwapped) {
                try {
                    registrar.handle(new Signal("INT"), prevInt);
                } catch (RuntimeException rollbackFailure) {
                    log.debug("signal_handler_install_rollback_failed message={}", rollbackFailure.getMessage());
                }
            }
            previousInt = null;
            previousTerm = null;
            installed = false;
            log.debug("signal_handler_install_skipped message={}", e.getMessage());
        }
    }

    private static void restore() {
        if (!installed) {
            return;
        }
        try {
            registrar.handle(new Signal("INT"), previousInt);
            registrar.handle(new Signal("TERM"), previousTerm);
        } catch (RuntimeException e) {
            log.debug("signal_handler_restore_failed message={}", e.getMessage());
        } finally {
            installed = false;
            previousInt = null;
            previousTerm = null;
        }
    }

    private static void onSignal(Signal signal) {
        CancellationToken token = active;
        if (token != null) {
            // Carry the signal identity through so the exit boundary can tell a supervisor SIGTERM
            // (143) from an interactive SIGINT (130); first-writer-wins CAS is unchanged.
            CancelSource source = "TERM".equals(signal.getName()) ? CancelSource.SIGTERM : CancelSource.SIGINT;
            token.cancel(StopReason.SIGNAL, source);
        }
    }
}
