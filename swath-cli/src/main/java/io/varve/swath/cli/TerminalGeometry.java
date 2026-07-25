/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import org.jline.terminal.spi.SystemStream;
import org.jline.terminal.spi.TerminalProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How wide the terminal on stderr is, for the one caller that needs it: {@link ProgressDisplay}'s
 * in-place redraw, which erases with a carriage return and a clear-to-end-of-line and therefore
 * must never emit a frame long enough to wrap. A wrapped frame occupies two physical rows and the
 * erase reaches only one, so the leftovers march down the screen. Width is a truncation bound and
 * nothing else — no layout is derived from it.
 *
 * <p><b>Why JLine, and why so little of it.</b> Terminal size comes from an {@code ioctl} whose
 * request value, {@code struct winsize} layout and libc calling convention differ per platform,
 * with {@code GetConsoleScreenBufferInfo} rather than {@code TIOCGWINSZ} on Windows. That is
 * exactly the code worth not owning. But JLine's usual entry point, {@code
 * TerminalBuilder.build()}, constructs a full interactive terminal: on a real tty it clears {@code
 * ISIG}, {@code ICANON} and {@code ECHO} and does not restore them on {@code close()}, which would
 * hand a swath run the power to leave an operator's shell with no echo and no Ctrl-C. A lister must
 * not do that, least of all one that runs for an hour and can be killed at any moment.
 *
 * <p>So this class deliberately stops one layer lower, at {@link
 * TerminalProvider#systemStreamWidth}, which answers the width question directly and constructs no
 * terminal at all. JLine never receives this process's streams, never installs a signal handler and
 * never touches terminal attributes — verified by comparing {@code stty -g} across a run.
 *
 * <p><b>Providers.</b> {@code ffm} is the one that ships: it is an {@code ioctl} through the JDK 25
 * FFM API, costs microseconds, and so is queried afresh for every frame — which is also why no
 * {@code WINCH} handler exists here. A resize is picked up by the next frame, and swath installs no
 * signal handler it does not need. {@code exec} forks {@code stty} and is the fallback for a JVM
 * started without {@code --enable-native-access} (the launcher scripts pass it, a bare {@code java
 * -jar} may not); at one query per frame its half-millisecond is affordable. The JNI provider is
 * excluded from the build, so it is absent by construction rather than by accident.
 *
 * <p><b>Unknown is a normal answer.</b> A pipe, a file, a terminal that will not say, or no working
 * provider all yield {@link #UNKNOWN}, and the display answers by staying with the plain
 * newline-terminated records it would have written anyway. Nothing here throws.
 *
 * <p>This is not a second opinion on {@link TerminalCapabilities}: that answers <em>is this fd a
 * terminal</em>, per fd, for {@code --format auto} and for the progress and summary gates, and it
 * answers without JLine on the classpath. This answers <em>how wide</em>, and only ever after the
 * former has already said yes.
 */
final class TerminalGeometry {

    private static final Logger log = LoggerFactory.getLogger(TerminalGeometry.class);

    /** No width could be determined — not a terminal, or no provider would say. */
    static final int UNKNOWN = -1;

    /**
     * Below this, a redraw is not worth attempting: a frame truncated to a dozen columns carries no
     * information, and the run is better served by the plain records that can at least wrap
     * readably. Chosen as the width under which even {@code listing · 1.2M objects} does not fit.
     */
    static final int MIN_USABLE_WIDTH = 24;

    private TerminalGeometry() {
    }

    /**
     * The width of the terminal on fd 2, or {@link #UNKNOWN}. Cheap enough to call once per frame,
     * which is how a resize is noticed without a {@code WINCH} handler.
     */
    static int stderrWidth() {
        TerminalProvider provider = Holder.PROVIDER;
        if (provider == null) {
            return UNKNOWN;
        }
        try {
            // Providers disagree about how they say "no": ffm answers 0, exec answers -1.
            int width = provider.systemStreamWidth(SystemStream.Error);
            return width > 0 ? width : UNKNOWN;
        } catch (RuntimeException e) {
            // A provider that worked at load time and fails now costs the redraw, never the run.
            return UNKNOWN;
        }
    }

    /**
     * The provider is resolved once, on first use, so a run that never redraws pays none of it —
     * the FFM linker bootstrap alone is tens of milliseconds, and it would otherwise land on every
     * {@code swath list} that only ever writes a summary.
     */
    private static final class Holder {

        private static final TerminalProvider PROVIDER = load();

        private static TerminalProvider load() {
            for (String name : new String[] {"ffm", "exec"}) {
                try {
                    TerminalProvider provider = TerminalProvider.load(name);
                    if (provider.systemStreamWidth(SystemStream.Error) > 0) {
                        return provider;
                    }
                } catch (Throwable t) {
                    // Absent artifact, no native access, a refusing platform: try the next, and if
                    // none answers let the display fall back. Throwable because a provider that
                    // cannot link raises an Error, not an exception.
                    log.debug("terminal_width_provider_unavailable provider={} message={}",
                            name, t.getMessage());
                }
            }
            return null;
        }
    }
}
