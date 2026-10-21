/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import javax.net.ssl.SSLSocketFactory;

/**
 * The JVM default TLS socket factory, reporting each connection to the gate. HttpURLConnection
 * resends an idempotent request once on a read failure, below the SDK; with keep-alive off every
 * send opens a connection, so a connection beyond the counted attempts is that resend.
 */
final class CountingSslSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegate = (SSLSocketFactory) SSLSocketFactory.getDefault();
    private final AttemptGate gate;
    private final AttemptGate.Kind kind;

    CountingSslSocketFactory(AttemptGate gate, AttemptGate.Kind kind) {
        this.gate = gate;
        this.kind = kind;
    }

    @Override public String[] getDefaultCipherSuites() { return delegate.getDefaultCipherSuites(); }

    @Override public String[] getSupportedCipherSuites() { return delegate.getSupportedCipherSuites(); }

    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
        gate.transmission(kind);
        return delegate.createSocket(socket, host, port, autoClose);
    }

    @Override
    public Socket createSocket() throws IOException {
        gate.transmission(kind);
        return delegate.createSocket();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        gate.transmission(kind);
        return delegate.createSocket(host, port);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        gate.transmission(kind);
        return delegate.createSocket(host, port, localHost, localPort);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        gate.transmission(kind);
        return delegate.createSocket(host, port);
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
            throws IOException {
        gate.transmission(kind);
        return delegate.createSocket(address, port, localAddress, localPort);
    }
}
