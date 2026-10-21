/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * The SDK's default {@link NetHttpTransport}, with every attempt counted before it is sent and
 * storage exchanges recorded. Credential refresh traffic uses an unrecorded instance. Each
 * attempt's timeouts are the request cap shortened by the deadline, response bytes are charged
 * as they are read, and TLS connections are counted so a transport resend is also gated.
 */
final class GcsGatedTransport extends HttpTransport {
    private static final java.util.concurrent.ScheduledExecutorService WATCHDOG =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "gcs-capture-watchdog");
                thread.setDaemon(true);
                return thread;
            });
    private static final Method BUILD_REQUEST = buildRequestMethod();

    private final HttpTransport delegate;
    private final AttemptGate gate;
    private final AttemptGate.Kind kind;
    private final ExchangeRecorder recorder;

    private GcsGatedTransport(AttemptGate gate, AttemptGate.Kind kind, ExchangeRecorder recorder) {
        this.gate = gate;
        this.kind = kind;
        this.recorder = recorder;
        this.delegate = new NetHttpTransport.Builder()
                .setSslSocketFactory(new CountingSslSocketFactory(gate, kind)).build();
    }

    static GcsGatedTransport storage(AttemptGate gate, ExchangeRecorder recorder) {
        return new GcsGatedTransport(gate, AttemptGate.Kind.STORAGE, recorder);
    }

    static GcsGatedTransport auth(AttemptGate gate, ExchangeRecorder bounds) {
        return new GcsGatedTransport(gate, AttemptGate.Kind.AUTH, bounds);
    }

    @Override
    public boolean supportsMethod(String method) throws IOException {
        return delegate.supportsMethod(method);
    }

    @Override
    protected LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        return new GatedRequest(method, url, delegateRequest(method, url));
    }

    @Override
    public void shutdown() throws IOException {
        delegate.shutdown();
    }

    private LowLevelHttpRequest delegateRequest(String method, String url) throws IOException {
        try {
            return (LowLevelHttpRequest) BUILD_REQUEST.invoke(delegate, method, url);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof IOException io) throw io;
            if (e.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new IOException(e.getCause());
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    // NetHttpTransport is final and exposes request construction only to subclasses. Calling
    // it reflectively keeps the SDK's real default transport instead of a lookalike.
    private static Method buildRequestMethod() {
        try {
            Method method = HttpTransport.class.getDeclaredMethod("buildRequest", String.class, String.class);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final class GatedRequest extends LowLevelHttpRequest {
        private final String method;
        private final String url;
        private final LowLevelHttpRequest request;
        private final List<ExchangeRecorder.Header> headers = new ArrayList<>();
        private int connectTimeout;
        private int readTimeout;
        private int writeTimeout;

        GatedRequest(String method, String url, LowLevelHttpRequest request) {
            this.method = method;
            this.url = url;
            this.request = request;
        }

        @Override
        public void addHeader(String name, String value) throws IOException {
            request.addHeader(name, value);
            headers.add(new ExchangeRecorder.Header(name, value));
        }

        @Override
        public void setTimeout(int connectTimeout, int readTimeout) {
            this.connectTimeout = connectTimeout;
            this.readTimeout = readTimeout;
        }

        @Override
        public void setWriteTimeout(int writeTimeout) {
            this.writeTimeout = writeTimeout;
        }

        private static int capped(int requested, int limit) {
            return requested <= 0 ? limit : Math.min(requested, limit);
        }

        @Override
        public LowLevelHttpResponse execute() throws IOException {
            if (kind == AttemptGate.Kind.STORAGE) {
                recorder.reserve();
            }
            gate.acquire(kind);
            int limit = gate.timeoutMillisNow();
            request.setTimeout(capped(connectTimeout, limit), capped(readTimeout, limit));
            request.setWriteTimeout(capped(writeTimeout, limit));
            request.setContentLength(getContentLength());
            request.setContentEncoding(getContentEncoding());
            request.setContentType(getContentType());
            request.setStreamingContent(getStreamingContent());
            long started = System.nanoTime();
            LowLevelHttpResponse response;
            try {
                response = request.execute();
            } catch (java.net.SocketTimeoutException e) {
                throw new CaptureBoundException("request_time");
            }
            long remainingMillis = limit - (System.nanoTime() - started) / 1_000_000;
            if (remainingMillis <= 0) {
                response.disconnect();
                throw new CaptureBoundException("request_time");
            }
            // A trickling body cannot outlive the attempt's total cap: the connection is closed then.
            java.util.concurrent.atomic.AtomicBoolean expired = new java.util.concurrent.atomic.AtomicBoolean();
            java.util.concurrent.ScheduledFuture<?> watchdog = WATCHDOG.schedule(() -> {
                expired.set(true);
                try {
                    response.disconnect();
                } catch (IOException ignored) {
                    // the read below fails and reports the bound
                }
            }, remainingMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
            byte[] wire;
            try {
                wire = recorder.readBounded(response.getContent(), gate);
            } catch (IOException | RuntimeException e) {
                response.disconnect();
                if (expired.get() || e instanceof java.net.SocketTimeoutException) {
                    throw new CaptureBoundException("request_time");
                }
                throw e;
            } finally {
                watchdog.cancel(false);
            }
            if (expired.get()) throw new CaptureBoundException("request_time");
            if (kind == AttemptGate.Kind.STORAGE) {
                List<ExchangeRecorder.Header> responseHeaders = new ArrayList<>();
                for (int i = 0; i < response.getHeaderCount(); i++) {
                    responseHeaders.add(new ExchangeRecorder.Header(response.getHeaderName(i),
                            response.getHeaderValue(i)));
                }
                recorder.record(method, url, headers, response.getStatusCode(), responseHeaders, wire);
            }
            gate.requireWithinDeadline();
            return new BufferedResponse(response, wire);
        }
    }

    /** Hands the SDK the exact wire bytes and encoding it would otherwise have streamed. */
    private static final class BufferedResponse extends LowLevelHttpResponse {
        private final LowLevelHttpResponse response;
        private final byte[] wire;

        BufferedResponse(LowLevelHttpResponse response, byte[] wire) {
            this.response = response;
            this.wire = wire;
        }

        @Override public InputStream getContent() { return new ByteArrayInputStream(wire); }
        @Override public String getContentEncoding() throws IOException { return response.getContentEncoding(); }
        @Override public long getContentLength() throws IOException { return response.getContentLength(); }
        @Override public String getContentType() throws IOException { return response.getContentType(); }
        @Override public String getStatusLine() throws IOException { return response.getStatusLine(); }
        @Override public int getStatusCode() throws IOException { return response.getStatusCode(); }
        @Override public String getReasonPhrase() throws IOException { return response.getReasonPhrase(); }
        @Override public int getHeaderCount() throws IOException { return response.getHeaderCount(); }
        @Override public String getHeaderName(int index) throws IOException { return response.getHeaderName(index); }
        @Override public String getHeaderValue(int index) throws IOException { return response.getHeaderValue(index); }
        @Override public void disconnect() throws IOException { response.disconnect(); }
    }
}
