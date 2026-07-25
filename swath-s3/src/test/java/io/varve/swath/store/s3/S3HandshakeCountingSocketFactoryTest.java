/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpHost;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.apache.internal.conn.SdkTlsSocketFactory;

/**
 * {@link S3HandshakeCountingSocketFactory} — the {@code swath.s3.pool.handshakes} recording
 * wrapper. No real network I/O: {@link #connectSocket}/{@link #createLayeredSocket} are exercised
 * against stub delegates so the counter/delegation ordering is pinned without opening a socket.
 */
class S3HandshakeCountingSocketFactoryTest {

    @Test
    void wrapperIsALayeredConnectionSocketFactory() {
        // Required so Apache's DefaultHttpClientConnectionOperator.upgrade() accepts this factory
        // for the https scheme; a plain ConnectionSocketFactory throws UnsupportedSchemeException
        // for any proxied HTTPS connection.
        assertThat(new S3HandshakeCountingSocketFactory(S3HandshakeCountingSocketFactory.defaultDelegate(),
                new RunMetrics(new SimpleMeterRegistry())))
                .isInstanceOf(LayeredConnectionSocketFactory.class);
    }

    @Test
    void connectSocketIncrementsHandshakeCounterAndDelegates() throws IOException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        AtomicBoolean delegated = new AtomicBoolean();
        Socket unconnectedSocket = new Socket();
        ConnectionSocketFactory delegate = new ConnectionSocketFactory() {
            @Override
            public Socket createSocket(HttpContext context) {
                return new Socket();
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host,
                    InetSocketAddress remoteAddress, InetSocketAddress localAddress,
                    HttpContext context) {
                delegated.set(true);
                return socket;
            }
        };
        S3HandshakeCountingSocketFactory wrapper = new S3HandshakeCountingSocketFactory(delegate, metrics);

        Socket result = wrapper.connectSocket(1000, unconnectedSocket, HttpHost.create("example.com"),
                new InetSocketAddress("127.0.0.1", 443), null, new BasicHttpContext());

        assertThat(result).isSameAs(unconnectedSocket);
        assertThat(delegated.get()).as("must delegate the real connect to the wrapped factory").isTrue();
        assertThat(registry.get("swath.s3.pool.handshakes").counter().count()).isEqualTo(1.0);
    }

    @Test
    void connectSocketThatThrowsDoesNotCountAHandshake() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        ConnectionSocketFactory delegate = new ConnectionSocketFactory() {
            @Override
            public Socket createSocket(HttpContext context) {
                return new Socket();
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host,
                    InetSocketAddress remoteAddress, InetSocketAddress localAddress,
                    HttpContext context) throws IOException {
                throw new IOException("connect refused");
            }
        };
        S3HandshakeCountingSocketFactory wrapper = new S3HandshakeCountingSocketFactory(delegate, metrics);

        assertThatThrownBy(() -> wrapper.connectSocket(1000, new Socket(), HttpHost.create("example.com"),
                new InetSocketAddress("127.0.0.1", 443), null, new BasicHttpContext()))
                .isInstanceOf(IOException.class);

        // A failed connect/handshake is already visible via swath.s3.pool.connection_aborted; it must
        // not also be counted as a completed handshake here.
        assertThat(registry.find("swath.s3.pool.handshakes").counter().count()).isEqualTo(0.0);
    }

    @Test
    void createSocketDelegatesWithoutCountingAHandshake() throws IOException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        Socket delegateSocket = new Socket();
        ConnectionSocketFactory delegate = new ConnectionSocketFactory() {
            @Override
            public Socket createSocket(HttpContext context) {
                return delegateSocket;
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host,
                    InetSocketAddress remoteAddress, InetSocketAddress localAddress,
                    HttpContext context) {
                throw new AssertionError("connectSocket should not be called by createSocket");
            }
        };
        S3HandshakeCountingSocketFactory wrapper = new S3HandshakeCountingSocketFactory(delegate, metrics);

        Socket result = wrapper.createSocket(new BasicHttpContext());

        assertThat(result).isSameAs(delegateSocket);
        // createSocket() only allocates the socket; the handshake happens in connectSocket() or
        // createLayeredSocket(), so this must not have counted anything yet.
        assertThat(registry.find("swath.s3.pool.handshakes").counter().count()).isEqualTo(0.0);
    }

    @Test
    void createLayeredSocketDelegatesAndCountsAHandshake() throws IOException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        AtomicBoolean delegated = new AtomicBoolean();
        Socket plainSocket = new Socket();
        Socket layeredSocket = new Socket();
        LayeredConnectionSocketFactory delegate = new LayeredConnectionSocketFactory() {
            @Override
            public Socket createSocket(HttpContext context) {
                throw new AssertionError("createSocket should not be called by createLayeredSocket");
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host,
                    InetSocketAddress remoteAddress, InetSocketAddress localAddress,
                    HttpContext context) {
                throw new AssertionError("connectSocket should not be called by createLayeredSocket");
            }

            @Override
            public Socket createLayeredSocket(Socket socket, String target, int port,
                    HttpContext context) {
                delegated.set(true);
                return layeredSocket;
            }
        };
        S3HandshakeCountingSocketFactory wrapper = new S3HandshakeCountingSocketFactory(delegate, metrics);

        Socket result = wrapper.createLayeredSocket(plainSocket, "example.com", 443, new BasicHttpContext());

        assertThat(result).isSameAs(layeredSocket);
        assertThat(delegated.get()).as("must delegate the layered TLS upgrade to the wrapped factory").isTrue();
        assertThat(registry.get("swath.s3.pool.handshakes").counter().count()).isEqualTo(1.0);
    }

    @Test
    void createLayeredSocketThatThrowsDoesNotCountAHandshake() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        LayeredConnectionSocketFactory delegate = new LayeredConnectionSocketFactory() {
            @Override
            public Socket createSocket(HttpContext context) {
                return new Socket();
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host,
                    InetSocketAddress remoteAddress, InetSocketAddress localAddress,
                    HttpContext context) {
                return socket;
            }

            @Override
            public Socket createLayeredSocket(Socket socket, String target, int port,
                    HttpContext context) throws IOException {
                throw new IOException("TLS handshake failed");
            }
        };
        S3HandshakeCountingSocketFactory wrapper = new S3HandshakeCountingSocketFactory(delegate, metrics);

        assertThatThrownBy(() -> wrapper.createLayeredSocket(new Socket(), "example.com", 443,
                new BasicHttpContext()))
                .isInstanceOf(IOException.class);

        assertThat(registry.find("swath.s3.pool.handshakes").counter().count()).isEqualTo(0.0);
    }

    @Test
    void createLayeredSocketThrowsAClearFailureWhenDelegateIsNotLayered() {
        ConnectionSocketFactory nonLayeredDelegate = new ConnectionSocketFactory() {
            @Override
            public Socket createSocket(HttpContext context) {
                return new Socket();
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host,
                    InetSocketAddress remoteAddress, InetSocketAddress localAddress,
                    HttpContext context) {
                return socket;
            }
        };
        S3HandshakeCountingSocketFactory wrapper =
                new S3HandshakeCountingSocketFactory(nonLayeredDelegate, new RunMetrics(new SimpleMeterRegistry()));

        assertThatThrownBy(() -> wrapper.createLayeredSocket(new Socket(), "example.com", 443,
                new BasicHttpContext()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("LayeredConnectionSocketFactory");
    }

    /**
     * {@link S3HandshakeCountingSocketFactory#defaultDelegate()} must build a real delegate WITHOUT
     * touching the network — it only assembles a local {@code SSLContext}/{@code SdkTlsSocketFactory}
     * object graph, never dials out.
     */
    @Test
    void defaultDelegateBuildsANonNullFactoryWithoutNetwork() {
        assertThat(S3HandshakeCountingSocketFactory.defaultDelegate()).isNotNull();
    }

    /**
     * Pins the {@code @SdkInternalApi} assumption the class javadoc calls out: {@code
     * defaultDelegate()} must return a {@code LayeredConnectionSocketFactory} whose concrete class is
     * exactly {@code SdkTlsSocketFactory}, so a silent AWS SDK drift away from that internal type is
     * caught here rather than only at runtime behind an HTTPS proxy.
     */
    @Test
    void defaultDelegateIsLayeredAndIsSdkTlsSocketFactory() {
        ConnectionSocketFactory delegate = S3HandshakeCountingSocketFactory.defaultDelegate();

        assertThat(delegate).isInstanceOf(LayeredConnectionSocketFactory.class);
        assertThat(delegate).isExactlyInstanceOf(SdkTlsSocketFactory.class);
    }
}
