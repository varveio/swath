/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.RunMetrics;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.http.SystemPropertyTlsKeyManagersProvider;
import software.amazon.awssdk.http.apache.internal.conn.SdkTlsSocketFactory;

/**
 * {@code swath.s3.pool.handshakes} — a {@code ConnectionSocketFactory} that delegates every call
 * to the same TLS socket factory {@code ApacheHttpClient} would build by default, and counts one
 * completed TCP-connect + TLS-handshake per fresh pooled connection: {@link #connectSocket} for a
 * direct (non-proxied) connection, or {@link #createLayeredSocket} for TLS layered over an
 * already-connected proxy tunnel. Apache's connection manager calls these only when opening a new
 * connection, never on lease/reuse, so this is a precise, cheap measure of handshake-churn rate —
 * a rate the {@code swath.s3.pool.*} snapshot gauges cannot see at all.
 *
 * <p><b>Must implement {@link LayeredConnectionSocketFactory}, not just {@code
 * ConnectionSocketFactory}:</b> the SDK's own default TLS factory ({@code SdkTlsSocketFactory}
 * extends {@code SSLConnectionSocketFactory}) implements it, and Apache's connection operator
 * throws {@code UnsupportedSchemeException} whenever the {@code https}-scheme socket factory is
 * not a {@code LayeredConnectionSocketFactory} — including behind an HTTPS proxy the client
 * auto-detects from the {@code https.proxyHost}/{@code https.proxyPort} system properties (or env,
 * in recent SDK versions) with no explicit client config. A plain {@code ConnectionSocketFactory}
 * wrapper here would make every request fail behind an HTTPS proxy the moment metrics are enabled.
 *
 * <p><b>Two mutually-exclusive completion paths, no double-count:</b> a direct connection calls
 * only this wrapper's {@link #connectSocket} — the delegate's {@code connectSocket} performs the
 * TLS handshake inline and never calls back into {@code createLayeredSocket}. A proxied HTTPS
 * connection first tunnels through the proxy in plain TCP (via {@code PlainConnectionSocketFactory},
 * not this factory) and then calls only this wrapper's {@link #createLayeredSocket} to layer TLS on
 * top of that already-connected socket. Exactly one of the two completes per connection, so
 * counting a completion in each is safe.
 *
 * <p><b>Count after success, not before:</b> the meter name is {@code ...handshakes} — a completed
 * handshake — so both methods increment only once the delegate's call returns normally. A delegate
 * that throws (TLS failure, connect refused, timeout) is not counted here; that failure is already
 * visible via {@code swath.s3.pool.connection_aborted}.
 *
 * <p><b>Why delegate rather than replace:</b> {@code ApacheHttpClient.Builder#socketFactory}
 * replaces the SDK's own default-TLS-factory construction wholesale — the escape hatch for a
 * caller supplying a custom trust store / client cert / proxy-aware factory — so installing a
 * plain {@link SSLConnectionSocketFactory} here would silently drop the SDK's own {@code
 * SdkTlsSocketFactory} behavior (its {@code connectSocket} wraps the connected socket to detect a
 * half-shutdown peer), a real behavior change disguised as an observability add. Instead {@link
 * #defaultDelegate()} builds the identical object graph {@code
 * ApacheHttpClient.DefaultBuilder.getPreferredSocketFactory} builds when no socket-factory / TLS
 * override is configured (none of which {@code S3ClientFactory} ever sets), and this class only
 * wraps that factory to count, never re-implements any TLS decision itself.
 *
 * <p><b>Caveat:</b> {@code SdkTlsSocketFactory} is {@code @SdkInternalApi} — it carries no
 * cross-version compatibility guarantee. This class depends on its current constructor shape and
 * its {@code extends SSLConnectionSocketFactory} (hence {@code LayeredConnectionSocketFactory})
 * supertype; an SDK bump that removes or renames it fails this module's compile rather than
 * silently drifting. If that happens, drop this counter rather than reimplement the internal TLS
 * wiring by hand.
 */
final class S3HandshakeCountingSocketFactory implements LayeredConnectionSocketFactory {

    private final ConnectionSocketFactory delegate;
    private final RunMetrics metrics;

    S3HandshakeCountingSocketFactory(ConnectionSocketFactory delegate, RunMetrics metrics) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    /**
     * Builds the same default {@code SdkTlsSocketFactory} {@code ApacheHttpClient} would build with
     * no socket-factory / TLS-provider override configured — see the class javadoc's "why delegate
     * rather than replace" note.
     *
     * <p>Pinned against AWS SDK v2 {@code software.amazon.awssdk:apache-client:2.31.78} (see
     * {@code gradle/libs.versions.toml}, {@code awssdk} version), whose {@code
     * ApacheHttpClient.DefaultBuilder.getPreferredSocketFactory}/{@code getSslContext} build exactly
     * this object graph: {@code SdkTlsSocketFactory(SSLContext, HostnameVerifier)} constructed from
     * a {@code TLS} {@link SSLContext} initialized with {@link SystemPropertyTlsKeyManagersProvider}
     * key managers, {@code null} trust managers, and {@code
     * SSLConnectionSocketFactory.getDefaultHostnameVerifier()}. Re-diff against that construction on
     * the next {@code awssdk} version bump.
     */
    static ConnectionSocketFactory defaultDelegate() {
        try {
            KeyManager[] keyManagers = SystemPropertyTlsKeyManagersProvider.create().keyManagers();
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, null, null);
            return new SdkTlsSocketFactory(sslContext, SSLConnectionSocketFactory.getDefaultHostnameVerifier());
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalStateException("Failed to build the default TLS socket factory", e);
        }
    }

    @Override
    public Socket createSocket(HttpContext context) throws IOException {
        return delegate.createSocket(context);
    }

    @Override
    public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host,
            InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context)
            throws IOException {
        Socket connected = delegate.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
        metrics.recordConnectionHandshake();
        return connected;
    }

    /**
     * The proxied-HTTPS completion path — see the class javadoc's "two mutually-exclusive completion
     * paths" note. {@code delegate} is always a {@link LayeredConnectionSocketFactory} in practice
     * (it is either {@link #defaultDelegate()}'s {@code SdkTlsSocketFactory}, which extends {@code
     * SSLConnectionSocketFactory}, or a caller-supplied factory installed for the same {@code https}
     * scheme, which Apache requires to be layered); the cast is guarded with a clear failure rather
     * than silently swallowing the layered call.
     */
    @Override
    public Socket createLayeredSocket(Socket socket, String target, int port, HttpContext context)
            throws IOException {
        if (!(delegate instanceof LayeredConnectionSocketFactory layeredDelegate)) {
            throw new IllegalStateException(
                    "S3HandshakeCountingSocketFactory delegate must be a LayeredConnectionSocketFactory "
                            + "to support proxied HTTPS connections, but was " + delegate.getClass().getName());
        }
        Socket layered = layeredDelegate.createLayeredSocket(socket, target, port, context);
        metrics.recordConnectionHandshake();
        return layered;
    }
}
