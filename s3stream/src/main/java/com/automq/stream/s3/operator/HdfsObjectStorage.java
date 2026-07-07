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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.utils.FutureUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * WebHDFS (HDFS HTTP v2) backed {@link ObjectStorage}, implemented as a pure REST client with no Hadoop dependency.
 * <p>
 * It extends {@link AbstractObjectStorage} so concurrency limiting, retry, bandwidth throttling and the
 * multipart-upload orchestration ({@link ProxyWriter}) are reused. Only the low level {@code doXxx} primitives are
 * implemented, each mapped onto a WebHDFS operation:
 * <ul>
 *     <li>range read      -&gt; {@code op=OPEN&offset&length}</li>
 *     <li>write           -&gt; temp {@code op=CREATE} then atomic {@code op=RENAME} (S3-like atomic publish)</li>
 *     <li>multipart part  -&gt; {@code op=CREATE} temp part file</li>
 *     <li>complete        -&gt; assemble parts (CREATE + APPEND) then atomic {@code op=RENAME&renameoptions=OVERWRITE}</li>
 *     <li>list            -&gt; recursive {@code op=LISTSTATUS}</li>
 *     <li>delete          -&gt; {@code op=DELETE}</li>
 *     <li>readiness       -&gt; {@code op=GETFILESTATUS}</li>
 * </ul>
 * URL format: {@code <endpoint>/data/<key>?op=<OP>}, where {@code endpoint} is the full WebHDFS base URL
 * (including {@code /webhdfs/v1/<subcluster>} and any directory) following the HDFS HTTP v2 spec.
 * <p>
 * BucketURI mapping ({@code <bucketId>@hdfs://<placeholder>?endpoint=<full WebHDFS base URL>&token=...}):
 * <ul>
 *     <li>the {@code <placeholder>} bucket name is not used for routing (a short label such as {@code automq} is
 *     recommended for readability; it may even be empty as in {@code hdfs://})</li>
 *     <li>{@code endpoint()} -&gt; full WebHDFS base, e.g.
 *     {@code https://<gateway-host>:<port>/webhdfs/v1/<subcluster>/<dir>/automq}</li>
 *     <li>extension {@code token} / env {@code AAD_TOKEN} -&gt; Entra ID bearer token</li>
 * </ul>
 * Example: {@code -3@hdfs://automq?endpoint=https://<gateway-host>:<port>/webhdfs/v1/<subcluster>/<dir>/automq&token=...}
 */
public class HdfsObjectStorage extends AbstractObjectStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsObjectStorage.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    public static final short BUCKET_ID = -3;

    private static final int MAX_REDIRECTS = 3;
    private static final String WEBHDFS_MARKER = "/webhdfs/v1";
    private static final String DATA_DIR = "data";
    private static final String MPU_DIR = "mpu";
    private static final String TMP_DIR = "tmp";
    /** Env var holding the Entra ID scope for the WebHDFS gateway API (e.g. {@code api://<app-id>/.default}). */
    private static final String TOKEN_SCOPE_ENV = "HDFS_TOKEN_SCOPE";

    private final HttpClient httpClient;
    /** Full WebHDFS REST base, e.g. {@code https://<gateway-host>:<port>/webhdfs/v1/<subcluster>/automq}. */
    private final String restBase;
    /** HDFS absolute path prefix (part after {@code /webhdfs/v1}), used for RENAME destination. */
    private final String hdfsPathPrefix;
    private final Duration requestTimeout;
    /** Supplies the current Entra ID bearer token; must be refreshable in production. */
    private final Supplier<String> tokenSupplier;
    /** Directories already MKDIRS'd, so repeated hot-path writes to the same dir skip a redundant MKDIRS. */
    private final Set<String> ensuredDirs = ConcurrentHashMap.newKeySet();

    public HdfsObjectStorage(BucketURI bucketURI, Map<String, String> tagging,
        NetworkBandwidthLimiter inboundLimiter, NetworkBandwidthLimiter outboundLimiter,
        boolean readWriteIsolate, boolean checkMode, String threadPrefix) {
        super(bucketURI, inboundLimiter, outboundLimiter, readWriteIsolate, checkMode, threadPrefix);
        // endpoint is the full WebHDFS base URL, e.g.
        // https://<gateway-host>:<port>/webhdfs/v1/<subcluster>/<dir>/automq
        this.restBase = trimTrailingSlash(bucketURI.endpoint());
        String path = URI.create(restBase).getRawPath();
        int markerIdx = path.indexOf(WEBHDFS_MARKER);
        // HDFS absolute path prefix (part after /webhdfs/v1), used for RENAME destination.
        this.hdfsPathPrefix = markerIdx >= 0 ? path.substring(markerIdx + WEBHDFS_MARKER.length()) : path;
        this.requestTimeout = Duration.ofMillis(
            Long.parseLong(bucketURI.extensionString(BucketURI.API_CALL_TIMEOUT_KEY, "30000")));
        this.tokenSupplier = defaultTokenSupplier(bucketURI);
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            // WebHDFS CREATE/OPEN may 307-redirect to a DataNode; we follow redirects manually to preserve the
            // request body and Authorization header (JDK HttpClient drops the body on 307/308).
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---- readiness ----

    @Override
    public boolean readinessCheck() {
        try {
            URI uri = URI.create(restBase + "?op=GETFILESTATUS");
            HttpResponse<byte[]> resp = send("GET", uri, null).join();
            return resp.statusCode() == 200 || resp.statusCode() == 404;
        } catch (Throwable e) {
            LOGGER.error("WebHDFS readiness check failed", e);
            return false;
        }
    }

    // ---- read ----

    @Override
    CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
        return openBytes(dataRel(path), start, end).thenApply(Unpooled::wrappedBuffer);
    }

    private CompletableFuture<byte[]> openBytes(String rel, long start, long end) {
        String[] params = (end == RANGE_READ_TO_END)
            ? new String[] {"offset", Long.toString(start)}
            : new String[] {"offset", Long.toString(start), "length", Long.toString(end - start)};
        return send("GET", opUri(rel, "OPEN", params), null).thenApply(resp -> {
            int sc = resp.statusCode();
            if (sc == 200) {
                return resp.body();
            }
            if (sc == 404) {
                throw new CompletionException(new ObjectNotExistException());
            }
            throw new CompletionException(httpError("OPEN " + rel, resp));
        });
    }

    // ---- single write ----

    @Override
    CompletableFuture<Void> doWrite(WriteOptions options, String path, ByteBuf data) {
        return writeAtomic(dataRel(path), toBytes(data));
    }

    /**
     * Atomically publishes an object: write the bytes to a unique temp file, then {@code op=RENAME} (an atomic
     * NameNode metadata operation) it onto the final path. This mirrors S3 PutObject's all-or-nothing visibility
     * that WAL recovery assumes: a broker killed mid-write can only leave a stray, never-listed temp file under
     * {@code tmp/}, never a truncated object under {@code data/} that would make {@code RecoverIterator} fail.
     */
    private CompletableFuture<Void> writeAtomic(String rel, byte[] body) {
        String stagingRel = tmpRel(UUID.randomUUID().toString());
        String parent = parentRel(rel);
        CompletableFuture<Void> ensureParent = parent == null
            ? CompletableFuture.completedFuture(null)
            : ensureDir(parent);
        return ensureParent
            .thenCompose(v -> create(stagingRel, body, true))
            .thenCompose(v -> renameTo(stagingRel, rel));
    }

    private CompletableFuture<Void> create(String rel, byte[] body, boolean overwrite) {
        URI uri = opUri(rel, "CREATE", "overwrite", Boolean.toString(overwrite), "data", "true");
        return oneShotWrite("PUT", uri, body, "CREATE " + rel, 201, 200);
    }

    private CompletableFuture<Void> append(String rel, byte[] body) {
        URI uri = opUri(rel, "APPEND", "data", "true");
        return oneShotWrite("POST", uri, body, "APPEND " + rel, 200);
    }

    /**
     * Single-request WebHDFS write: sends the payload inline with {@code data=true} and {@code application/octet-stream},
     * which an HttpFS-style gateway accepts directly (one round-trip). If instead the gateway is a classic WebHDFS that
     * replies with a redirect to a DataNode, the body is streamed to that {@code Location} as a fallback.
     */
    private CompletableFuture<Void> oneShotWrite(String method, URI uri, byte[] body, String op, int... okCodes) {
        return sendRaw(method, uri, body, "application/octet-stream").thenCompose(resp -> {
            String location = writeRedirectLocation(resp);
            if (location != null) {
                return sendRaw(method, URI.create(location), body, "application/octet-stream")
                    .thenAccept(dataResp -> expectOk(dataResp, op, okCodes));
            }
            expectOk(resp, op, okCodes);
            return CompletableFuture.completedFuture(null);
        });
    }

    /** Extracts the DataNode upload URL from either a {@code noredirect} JSON body or a 3xx {@code Location} header. */
    private static String writeRedirectLocation(HttpResponse<byte[]> resp) {
        int sc = resp.statusCode();
        if (sc == 307 || sc == 308 || sc == 301 || sc == 302 || sc == 303) {
            return resp.headers().firstValue("Location").orElse(null);
        }
        if (sc == 200 && resp.body() != null && resp.body().length > 0) {
            try {
                JsonNode loc = JSON.readTree(resp.body()).path("Location");
                if (!loc.isMissingNode() && StringUtils.isNotEmpty(loc.asText())) {
                    return loc.asText();
                }
            } catch (IOException ignored) {
                // Fall through to null: the caller reports the original response as an error.
            }
        }
        return null;
    }

    // ---- multipart ----

    @Override
    CompletableFuture<String> doCreateMultipartUpload(WriteOptions options, String path) {
        // WebHDFS CREATE auto-creates parent directories, so no server call is required here.
        return CompletableFuture.completedFuture(UUID.randomUUID().toString());
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf part) {
        String partRel = partRel(uploadId, partNumber);
        return create(partRel, toBytes(part), true)
            .thenApply(v -> new ObjectStorageCompletedPart(partNumber, partRel, null));
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPartCopy(WriteOptions options, String sourcePath, String path,
        long start, long end, String uploadId, int partNumber) {
        String partRel = partRel(uploadId, partNumber);
        // WebHDFS has no server-side ranged copy; read the source range and write it as a new part through the gateway.
        return openBytes(dataRel(sourcePath), start, end)
            .thenCompose(bytes -> create(partRel, bytes, true))
            .thenApply(v -> new ObjectStorageCompletedPart(partNumber, partRel, null));
    }

    @Override
    CompletableFuture<Void> doCompleteMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts) {
        List<ObjectStorageCompletedPart> ordered = new ArrayList<>(parts);
        ordered.sort(Comparator.comparingInt(ObjectStorageCompletedPart::getPartNumber));

        String stagingRel = tmpRel(uploadId + "-assembled");
        // Assemble parts into a staging file via CREATE(first) + APPEND(rest), then atomically rename to the target.
        // NOTE: this reads every part back through the gateway. For block-aligned parts, server-side op=CONCAT would
        // avoid the extra read IO; left as a future optimization because CONCAT requires full-block source files.
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int i = 0; i < ordered.size(); i++) {
            String partRel = ordered.get(i).getPartId();
            boolean first = i == 0;
            chain = chain.thenCompose(v -> openBytes(partRel, 0, RANGE_READ_TO_END)
                .thenCompose(bytes -> first ? create(stagingRel, bytes, true) : append(stagingRel, bytes)));
        }
        return chain
            .thenCompose(v -> rename(stagingRel, dataRel(path)))
            .thenCompose(v -> deletePath(mpuDirRel(uploadId), true).exceptionally(ex -> {
                LOGGER.warn("Failed to clean up multipart temp dir {}", uploadId, ex);
                return null;
            }));
    }

    private CompletableFuture<Void> rename(String fromRel, String toRel) {
        // WebHDFS RENAME (unlike CREATE) does not auto-create the destination's parent directory, so ensure it exists.
        String parent = parentRel(toRel);
        CompletableFuture<Void> ensureParent = parent == null
            ? CompletableFuture.completedFuture(null)
            : ensureDir(parent);
        return ensureParent.thenCompose(v -> renameTo(fromRel, toRel));
    }

    /** WebHDFS RENAME with OVERWRITE; the destination parent must already exist (see {@link #ensureDir}). */
    private CompletableFuture<Void> renameTo(String fromRel, String toRel) {
        URI uri = opUri(fromRel, "RENAME",
            "destination", hdfsAbsPath(toRel),
            "renameoptions", "OVERWRITE");
        return send("PUT", uri, null).thenAccept(resp -> expectOk(resp, "RENAME " + fromRel, 200));
    }

    /** MKDIRS the directory once and cache it, so hot-path writes to the same dir skip the extra request. */
    private CompletableFuture<Void> ensureDir(String rel) {
        if (ensuredDirs.contains(rel)) {
            return CompletableFuture.completedFuture(null);
        }
        return mkdirs(rel).thenRun(() -> ensuredDirs.add(rel));
    }

    private CompletableFuture<Void> mkdirs(String rel) {
        // MKDIRS creates all missing intermediate directories and is idempotent (succeeds if the path already exists).
        return send("PUT", opUri(rel, "MKDIRS"), null).thenAccept(resp -> expectOk(resp, "MKDIRS " + rel, 200));
    }

    // ---- delete ----

    @Override
    CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (String key : objectKeys) {
            chain = chain.thenCompose(v -> deletePath(dataRel(key), false));
        }
        return chain;
    }

    private CompletableFuture<Void> deletePath(String rel, boolean recursive) {
        URI uri = opUri(rel, "DELETE", "recursive", Boolean.toString(recursive));
        return send("DELETE", uri, null).thenAccept(resp -> {
            // 404 means already gone; treat as success for idempotent delete.
            if (resp.statusCode() != 200 && resp.statusCode() != 404) {
                throw new CompletionException(httpError("DELETE " + rel, resp));
            }
        });
    }

    // ---- list ----

    @Override
    CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        List<ObjectInfo> result = new ArrayList<>();
        return listRecursive(DATA_DIR, prefix, result).thenApply(v -> result);
    }

    private CompletableFuture<Void> listRecursive(String rel, String keyPrefix, List<ObjectInfo> out) {
        return send("GET", opUri(rel, "LISTSTATUS"), null).thenCompose(resp -> {
            if (resp.statusCode() == 404) {
                return CompletableFuture.completedFuture(null);
            }
            if (resp.statusCode() != 200) {
                return FutureUtil.failedFuture(httpError("LISTSTATUS " + rel, resp));
            }
            List<JsonNode> entries = parseFileStatuses(resp.body());
            CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
            for (JsonNode node : entries) {
                String suffix = node.path("pathSuffix").asText();
                String childRel = suffix.isEmpty() ? rel : rel + "/" + suffix;
                if ("DIRECTORY".equals(node.path("type").asText())) {
                    chain = chain.thenCompose(v -> listRecursive(childRel, keyPrefix, out));
                } else {
                    String key = childRel.substring(DATA_DIR.length() + 1);
                    if (key.startsWith(keyPrefix)) {
                        out.add(new ObjectInfo(bucketId(), key,
                            node.path("modificationTime").asLong(), node.path("length").asLong()));
                    }
                }
            }
            return chain;
        });
    }

    private static List<JsonNode> parseFileStatuses(byte[] body) {
        try {
            JsonNode arr = JSON.readTree(body).path("FileStatuses").path("FileStatus");
            List<JsonNode> list = new ArrayList<>();
            if (arr.isArray()) {
                arr.forEach(list::add);
            }
            return list;
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    // ---- retry strategy ----

    @Override
    Pair<RetryStrategy, Throwable> toRetryStrategyAndCause(Throwable ex, S3Operation operation) {
        Throwable cause = FutureUtil.cause(ex);
        RetryStrategy strategy = RetryStrategy.RETRY;
        if (cause instanceof ObjectNotExistException
            || cause instanceof IllegalArgumentException
            || cause instanceof UnsupportedOperationException) {
            strategy = RetryStrategy.ABORT;
        } else if (cause instanceof WebHdfsException) {
            int sc = ((WebHdfsException) cause).statusCode;
            // 4xx (except throttling/timeout) are not retriable; 5xx and connectivity errors are.
            if (sc >= 400 && sc < 500 && sc != 408 && sc != 429) {
                strategy = RetryStrategy.ABORT;
            }
        }
        return Pair.of(strategy, cause);
    }

    @Override
    void doClose() {
        // JDK HttpClient has no explicit close (its executor is a daemon); nothing to release.
    }

    // ---- HTTP plumbing ----

    private CompletableFuture<HttpResponse<byte[]>> send(String method, URI uri, byte[] body) {
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
                if ((sc == 307 || sc == 308 || sc == 301 || sc == 302 || sc == 303) && redirectCount < MAX_REDIRECTS) {
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
        throw new CompletionException(httpError(op, resp));
    }

    private static WebHdfsException httpError(String op, HttpResponse<byte[]> resp) {
        String body = resp.body() == null ? "" : new String(resp.body(), StandardCharsets.UTF_8);
        return new WebHdfsException(resp.statusCode(),
            String.format("WebHDFS %s failed, status=%d, body=%s", op, resp.statusCode(), body));
    }

    // ---- path helpers ----

    private URI opUri(String rel, String op, String... params) {
        StringBuilder sb = new StringBuilder(restBase)
            .append('/').append(rel)
            .append("?op=").append(op);
        for (int i = 0; i + 1 < params.length; i += 2) {
            sb.append('&').append(params[i]).append('=').append(params[i + 1]);
        }
        return URI.create(sb.toString());
    }

    /** HDFS absolute path for {@code rel}, e.g. {@code /<subcluster>/automq/data/<key>}. */
    private String hdfsAbsPath(String rel) {
        return hdfsPathPrefix + "/" + rel;
    }

    private String dataRel(String key) {
        return DATA_DIR + "/" + key;
    }

    private String partRel(String uploadId, int partNumber) {
        return MPU_DIR + "/" + uploadId + "/" + partNumber;
    }

    private String mpuDirRel(String uploadId) {
        return MPU_DIR + "/" + uploadId;
    }

    private String tmpRel(String name) {
        return TMP_DIR + "/" + name;
    }

    private static String parentRel(String rel) {
        int idx = rel.lastIndexOf('/');
        return idx <= 0 ? null : rel.substring(0, idx);
    }

    private static byte[] toBytes(ByteBuf data) {
        byte[] bytes = new byte[data.readableBytes()];
        data.getBytes(data.readerIndex(), bytes);
        return bytes;
    }

    private static Supplier<String> defaultTokenSupplier(BucketURI bucketURI) {
        // 1) An explicit static token (dev/tests) takes precedence.
        String token = bucketURI.extensionString("token", null);
        if (StringUtils.isNotBlank(token)) {
            return () -> token;
        }
        // 2) Azure Workload Identity federation: exchange the projected token for a refreshing Entra ID token.
        if (WorkloadIdentityTokenProvider.isAvailable()) {
            String scope = bucketURI.extensionString("tokenScope", System.getenv(TOKEN_SCOPE_ENV));
            if (StringUtils.isBlank(scope)) {
                throw new IllegalStateException(
                    "Azure Workload Identity requires a token scope: set BucketURI 'tokenScope' or env " + TOKEN_SCOPE_ENV);
            }
            LOGGER.info("Using Azure Workload Identity for WebHDFS auth, scope={}", scope);
            return WorkloadIdentityTokenProvider.fromEnvironment(scope);
        }
        // 3) Fallback to a static token from the environment (non-refreshing).
        return () -> {
            String env = System.getenv("AAD_TOKEN");
            if (StringUtils.isBlank(env)) {
                throw new IllegalStateException(
                    "No Entra ID token: set BucketURI 'token', configure Azure Workload Identity, or set env AAD_TOKEN");
            }
            return env;
        };
    }

    private static String trimTrailingSlash(String s) {
        return s != null && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    /** WebHDFS REST error carrying the HTTP status code for retry classification. */
    static final class WebHdfsException extends IOException {
        private final int statusCode;

        WebHdfsException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }
    }

    public static class Builder {
        private BucketURI bucketURI;
        private Map<String, String> tagging;
        private NetworkBandwidthLimiter inboundLimiter;
        private NetworkBandwidthLimiter outboundLimiter;
        private boolean readWriteIsolate;
        private boolean checkMode;
        private String threadPrefix;

        public Builder bucket(BucketURI bucketURI) {
            this.bucketURI = bucketURI;
            return this;
        }

        public Builder tagging(Map<String, String> tagging) {
            this.tagging = tagging;
            return this;
        }

        public Builder inboundLimiter(NetworkBandwidthLimiter inboundLimiter) {
            this.inboundLimiter = inboundLimiter;
            return this;
        }

        public Builder outboundLimiter(NetworkBandwidthLimiter outboundLimiter) {
            this.outboundLimiter = outboundLimiter;
            return this;
        }

        public Builder readWriteIsolate(boolean readWriteIsolate) {
            this.readWriteIsolate = readWriteIsolate;
            return this;
        }

        public Builder checkMode(boolean checkMode) {
            this.checkMode = checkMode;
            return this;
        }

        public Builder threadPrefix(String threadPrefix) {
            this.threadPrefix = threadPrefix;
            return this;
        }

        public HdfsObjectStorage build() {
            return new HdfsObjectStorage(bucketURI, tagging, inboundLimiter, outboundLimiter,
                readWriteIsolate, checkMode, threadPrefix);
        }
    }
}
