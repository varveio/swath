/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/**
 * The JVM default TLS context, reporting each TLS engine to the gate. java.net.http creates one
 * engine per connection and resends a GET once on a new connection when the first one ends
 * before any response byte, below the SDK; that extra connection is counted and gated here.
 */
final class CountingSslContext {
    private CountingSslContext() {
    }

    static SSLContext create(AttemptGate gate, AttemptGate.Kind kind) {
        SSLContext base;
        try {
            base = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no default TLS context", e);
        }
        SSLContextSpi spi = new SSLContextSpi() {
            @Override
            protected void engineInit(KeyManager[] keys, TrustManager[] trust, SecureRandom random)
                    throws KeyManagementException {
                throw new KeyManagementException("the default TLS context is already initialized");
            }

            @Override protected SSLSocketFactory engineGetSocketFactory() { return base.getSocketFactory(); }

            @Override protected SSLServerSocketFactory engineGetServerSocketFactory() {
                return base.getServerSocketFactory();
            }

            @Override
            protected SSLEngine engineCreateSSLEngine() {
                gate.transmission(kind);
                return base.createSSLEngine();
            }

            @Override
            protected SSLEngine engineCreateSSLEngine(String host, int port) {
                gate.transmission(kind);
                return base.createSSLEngine(host, port);
            }

            @Override protected SSLSessionContext engineGetServerSessionContext() {
                return base.getServerSessionContext();
            }

            @Override protected SSLSessionContext engineGetClientSessionContext() {
                return base.getClientSessionContext();
            }

            @Override protected SSLParameters engineGetDefaultSSLParameters() { return base.getDefaultSSLParameters(); }

            @Override protected SSLParameters engineGetSupportedSSLParameters() {
                return base.getSupportedSSLParameters();
            }
        };
        return new SSLContext(spi, base.getProvider(), base.getProtocol()) { };
    }
}
