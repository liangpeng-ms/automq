/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.webhdfs;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * Asynchronous WebHDFS (HDFS HTTP v2) client backing the AutoMQ HDFS object-storage / WAL backend. It exposes the
 * low-level atomic REST operations (CREATE / APPEND / OPEN / RENAME / MKDIRS / DELETE / LISTSTATUS / GETFILESTATUS) as
 * {@link CompletableFuture}s and owns the JDK {@link HttpClient} used to talk to the gateway.
 * <p>
 * It is the async counterpart of the synchronous {@link WebHdfsClient} (which backs Iceberg's blocking {@code FileIO}).
 * All higher-level orchestration - the temp-CREATE + RENAME atomic publish, multipart assembly, the bucketed/flattened
 * key layout, the recursive-list tree walk, the ensured-directory cache and retry classification - lives in the caller
 * ({@code HdfsObjectStorage}); every method here operates on already-composed <em>relative</em> paths.
 * <p>
 * Non-2xx/expected responses surface as {@link WebHdfsException} (carrying the HTTP status), so the caller can map a
 * read 404 to its own not-exist signal and classify retries uniformly.
 */
public class AsyncWebHdfsClient {
    /** Sentinel for {@link #open} meaning "read to the end of the file" (no {@code length} parameter). */
    public static final long READ_TO_END = -1L;

    private static final int MAX_REDIRECTS = 3;

    private final HttpClient httpClient;
    /** Supplies the current Entra ID bearer token; must be refreshable in production. */
    private final Supplier<String> tokenSupplier;
    /** Full WebHDFS REST base, e.g. {@code https://<gateway-host>:<port>/webhdfs/v1/<subcluster>/automq}. */
    private final String restBase;
    /** HDFS absolute path prefix (part after {@code /webhdfs/v1}), used for RENAME destination. */
    private final String hdfsPathPrefix;
    private final Duration requestTimeout;

    public AsyncWebHdfsClient(Supplier<String> tokenSupplier, String restBase, String hdfsPathPrefix,
        Duration requestTimeout) {
        this.tokenSupplier = tokenSupplier;
        this.restBase = restBase;
        this.hdfsPathPrefix = hdfsPathPrefix;
        this.requestTimeout = requestTimeout;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            // WebHDFS CREATE/OPEN may 307-redirect to a DataNode; we follow redirects manually to preserve the
            // request body and Authorization header (JDK HttpClient drops the body on 307/308).
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    }

    // ---- atomic operations ----

    public CompletableFuture<Void> create(String rel, byte[] body, boolean overwrite) {
        URI uri = opUri(rel, "CREATE", "overwrite", Boolean.toString(overwrite), "data", "true");
        return oneShotWrite("PUT", uri, body, "CREATE " + rel, 201, 200);
    }

    public CompletableFuture<Void> append(String rel, byte[] body) {
        URI uri = opUri(rel, "APPEND", "data", "true");
        return oneShotWrite("POST", uri, body, "APPEND " + rel, 200);
    }

    /**
     * Reads a byte range via {@code op=OPEN&offset&length}; {@code end == }{@link #READ_TO_END} reads to end of file.
     * A non-200 response (including 404) surfaces as {@link WebHdfsException}; the caller decides how to map it.
     */
    public CompletableFuture<byte[]> open(String rel, long start, long end) {
        String[] params = (end == READ_TO_END)
            ? new String[] {"offset", Long.toString(start)}
            : new String[] {"offset", Long.toString(start), "length", Long.toString(end - start)};
        return send("GET", opUri(rel, "OPEN", params), null).thenApply(resp -> {
            if (resp.statusCode() == 200) {
                return resp.body();
            }
            throw new CompletionException(error("OPEN " + rel, resp));
        });
    }

    /** WebHDFS RENAME with OVERWRITE; the destination parent must already exist (see the caller's {@code ensureDir}). */
    public CompletableFuture<Void> renameTo(String fromRel, String toRel) {
        URI uri = opUri(fromRel, "RENAME",
            "destination", hdfsAbsPath(toRel),
            "renameoptions", "OVERWRITE");
        return send("PUT", uri, null).thenAccept(resp -> expectOk(resp, "RENAME " + fromRel, 200));
    }

    public CompletableFuture<Void> mkdirs(String rel) {
        // MKDIRS creates all missing intermediate directories and is idempotent (succeeds if the path already exists).
        return send("PUT", opUri(rel, "MKDIRS"), null).thenAccept(resp -> expectOk(resp, "MKDIRS " + rel, 200));
    }

    public CompletableFuture<Void> deletePath(String rel, boolean recursive) {
        URI uri = opUri(rel, "DELETE", "recursive", Boolean.toString(recursive));
        return send("DELETE", uri, null).thenAccept(resp -> {
            // 404 means already gone; treat as success for idempotent delete.
            if (resp.statusCode() != 200 && resp.statusCode() != 404) {
                throw new CompletionException(error("DELETE " + rel, resp));
            }
        });
    }

    /** Raw {@code op=LISTSTATUS} response; the caller parses the JSON and walks the directory tree. */
    public CompletableFuture<HttpResponse<byte[]>> listStatus(String rel) {
        return send("GET", opUri(rel, "LISTSTATUS"), null);
    }

    /** Raw {@code op=GETFILESTATUS} response; {@code rel} may be empty to target the base path (readiness check). */
    public CompletableFuture<HttpResponse<byte[]>> getFileStatus(String rel) {
        return send("GET", opUri(rel, "GETFILESTATUS"), null);
    }

    // ---- write handshake ----

    /**
     * Single-request WebHDFS write: sends the payload inline with {@code data=true} and {@code application/octet-stream},
     * which an HttpFS-style gateway accepts directly (one round-trip). If instead the gateway is a classic WebHDFS that
     * replies with a redirect to a DataNode, the body is streamed to that {@code Location} as a fallback.
     */
    private CompletableFuture<Void> oneShotWrite(String method, URI uri, byte[] body, String op, int... okCodes) {
        return sendRaw(method, uri, body, "application/octet-stream").thenCompose(resp -> {
            String location = WebHdfsProtocol.writeRedirectLocation(resp);
            if (location != null) {
                return sendRaw(method, URI.create(location), body, "application/octet-stream")
                    .thenAccept(dataResp -> expectOk(dataResp, op, okCodes));
            }
            expectOk(resp, op, okCodes);
            return CompletableFuture.completedFuture(null);
        });
    }

    // ---- HTTP plumbing ----

    /** Auto-follows redirects (used by ops that consume the raw response, e.g. LISTSTATUS/GETFILESTATUS/RENAME). */
    public CompletableFuture<HttpResponse<byte[]>> send(String method, URI uri, byte[] body) {
        return send(method, uri, body, 0);
    }

    /**
     * Single HTTP exchange with no automatic redirect following, used for the two-phase WebHDFS write handshake where
     * each phase must be controlled explicitly (the redirect target already carries its own auth token).
     */
    private CompletableFuture<HttpResponse<byte[]>> sendRaw(String method, URI uri, byte[] body, String contentType) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
            .timeout(requestTimeout)
            .header("Authorization", "Bearer " + tokenSupplier.get());
        if (contentType != null) {
            builder.header("Content-Type", contentType);
        }
        builder.method(method, body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofByteArray(body));
        return httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private CompletableFuture<HttpResponse<byte[]>> send(String method, URI uri, byte[] body, int redirectCount) {
        HttpRequest.BodyPublisher publisher = body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofByteArray(body);
        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(requestTimeout)
            .header("Authorization", "Bearer " + tokenSupplier.get())
            .method(method, publisher)
            .build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenCompose(resp -> {
                int sc = resp.statusCode();
                if (WebHdfsProtocol.isRedirect(sc) && redirectCount < MAX_REDIRECTS) {
                    Optional<String> location = resp.headers().firstValue("Location");
                    if (location.isPresent()) {
                        return send(method, URI.create(location.get()), body, redirectCount + 1);
                    }
                }
                return CompletableFuture.completedFuture(resp);
            });
    }

    private static void expectOk(HttpResponse<byte[]> resp, String op, int... okCodes) {
        for (int ok : okCodes) {
            if (resp.statusCode() == ok) {
                return;
            }
        }
        throw new CompletionException(error(op, resp));
    }

    private static WebHdfsException error(String op, HttpResponse<byte[]> resp) {
        return new WebHdfsException(resp.statusCode(),
            WebHdfsProtocol.errorMessage(op, resp.statusCode(), resp.body()));
    }

    // ---- URI helpers ----

    private URI opUri(String rel, String op, String... params) {
        // An empty rel targets restBase itself (readiness GETFILESTATUS): no "/rel" segment, matching the original
        // "restBase?op=..." form exactly. A non-empty rel yields "restBase/rel?op=...".
        StringBuilder sb = new StringBuilder(restBase);
        if (!rel.isEmpty()) {
            sb.append('/').append(rel);
        }
        sb.append("?op=").append(op);
        WebHdfsProtocol.appendParams(sb, params);
        return URI.create(sb.toString());
    }

    /** HDFS absolute path for {@code rel}, e.g. {@code /<subcluster>/automq/data/<key>}. */
    private String hdfsAbsPath(String rel) {
        return hdfsPathPrefix + "/" + rel;
    }
}
