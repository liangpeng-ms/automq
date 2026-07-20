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

import com.automq.hdfs.token.WorkloadIdentityTokenUtil;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.webhdfs.AsyncWebHdfsClient;
import com.automq.stream.s3.webhdfs.WebHdfsException;
import com.automq.stream.s3.webhdfs.WebHdfsProtocol;
import com.automq.stream.utils.FutureUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
 * The low-level WebHDFS REST exchanges (redirect handling, the two-phase write handshake, per-op error mapping) live in
 * {@link AsyncWebHdfsClient}; this class keeps the orchestration on top of them: the atomic temp-CREATE + RENAME publish,
 * multipart assembly, the bucketed/flattened key layout, the recursive-list tree walk, the ensured-directory cache and
 * the retry classification.
 * <p>
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

    private static final String WEBHDFS_MARKER = "/webhdfs/v1";
    private static final String DATA_DIR = "data";
    private static final String MPU_DIR = "mpu";
    private static final String TMP_DIR = "tmp";
    /** Separator that replaces '/' when a logical key is flattened into a single HDFS filename (see {@link #dataRel}). */
    private static final char KEY_SEP = '~';

    /** Low-level WebHDFS REST client owning the {@code HttpClient}, token supplier and atomic op implementations. */
    private final AsyncWebHdfsClient client;
    /** Directories already MKDIRS'd, so repeated hot-path writes to the same dir skip a redundant MKDIRS. */
    private final Set<String> ensuredDirs = ConcurrentHashMap.newKeySet();

    public HdfsObjectStorage(BucketURI bucketURI, Map<String, String> tagging,
        NetworkBandwidthLimiter inboundLimiter, NetworkBandwidthLimiter outboundLimiter,
        boolean readWriteIsolate, boolean checkMode, String threadPrefix) {
        super(bucketURI, inboundLimiter, outboundLimiter, readWriteIsolate, checkMode, threadPrefix);
        // endpoint is the full WebHDFS base URL, e.g.
        // https://<gateway-host>:<port>/webhdfs/v1/<subcluster>/<dir>/automq
        String restBase = trimTrailingSlash(bucketURI.endpoint());
        String path = URI.create(restBase).getRawPath();
        int markerIdx = path.indexOf(WEBHDFS_MARKER);
        // HDFS absolute path prefix (part after /webhdfs/v1), used for RENAME destination.
        String hdfsPathPrefix = markerIdx >= 0 ? path.substring(markerIdx + WEBHDFS_MARKER.length()) : path;
        Duration requestTimeout = Duration.ofMillis(
            Long.parseLong(bucketURI.extensionString(BucketURI.API_CALL_TIMEOUT_KEY, "30000")));
        Supplier<String> tokenSupplier = defaultTokenSupplier(bucketURI);
        this.client = new AsyncWebHdfsClient(tokenSupplier, restBase, hdfsPathPrefix, requestTimeout);
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---- readiness ----

    @Override
    public boolean readinessCheck() {
        try {
            HttpResponse<byte[]> resp = client.getFileStatus("").join();
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

    /**
     * Ranged OPEN through the client, translating a 404 into {@link ObjectNotExistException} (the not-exist signal the
     * ObjectStorage contract and WAL recovery expect); any other WebHDFS error propagates unchanged for retry
     * classification.
     */
    private CompletableFuture<byte[]> openBytes(String rel, long start, long end) {
        return client.open(rel, start, end).exceptionally(ex -> {
            Throwable cause = FutureUtil.cause(ex);
            if (cause instanceof WebHdfsException && ((WebHdfsException) cause).statusCode() == 404) {
                throw new CompletionException(new ObjectNotExistException());
            }
            throw new CompletionException(cause);
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
            .thenCompose(v -> client.create(stagingRel, body, true))
            .thenCompose(v -> client.renameTo(stagingRel, rel));
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
        return client.create(partRel, toBytes(part), true)
            .thenApply(v -> new ObjectStorageCompletedPart(partNumber, partRel, null));
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPartCopy(WriteOptions options, String sourcePath, String path,
        long start, long end, String uploadId, int partNumber) {
        String partRel = partRel(uploadId, partNumber);
        // WebHDFS has no server-side ranged copy; read the source range and write it as a new part through the gateway.
        return openBytes(dataRel(sourcePath), start, end)
            .thenCompose(bytes -> client.create(partRel, bytes, true))
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
                .thenCompose(bytes -> first ? client.create(stagingRel, bytes, true) : client.append(stagingRel, bytes)));
        }
        return chain
            .thenCompose(v -> rename(stagingRel, dataRel(path)))
            .thenCompose(v -> client.deletePath(mpuDirRel(uploadId), true).exceptionally(ex -> {
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
        return ensureParent.thenCompose(v -> client.renameTo(fromRel, toRel));
    }

    /** MKDIRS the directory once and cache it, so hot-path writes to the same dir skip the extra request. */
    private CompletableFuture<Void> ensureDir(String rel) {
        if (ensuredDirs.contains(rel)) {
            return CompletableFuture.completedFuture(null);
        }
        return client.mkdirs(rel).thenRun(() -> ensuredDirs.add(rel));
    }

    // ---- delete ----

    @Override
    CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (String key : objectKeys) {
            chain = chain.thenCompose(v -> client.deletePath(dataRel(key), false));
        }
        return chain;
    }

    // ---- list ----

    @Override
    CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        List<ObjectInfo> result = new ArrayList<>();
        return listRecursive(DATA_DIR, prefix, result).thenApply(v -> result);
    }

    private CompletableFuture<Void> listRecursive(String rel, String keyPrefix, List<ObjectInfo> out) {
        return client.listStatus(rel).thenCompose(resp -> {
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
                    // The filename is the whole flattened key (see dataRel); decode it back to the logical key.
                    String key = decodeKey(suffix);
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

    /** Uniform WebHDFS error carrying the HTTP status, used for the LISTSTATUS tree walk's non-2xx responses. */
    private static WebHdfsException httpError(String op, HttpResponse<byte[]> resp) {
        return new WebHdfsException(resp.statusCode(),
            WebHdfsProtocol.errorMessage(op, resp.statusCode(), resp.body()));
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
            int sc = ((WebHdfsException) cause).statusCode();
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

    // ---- path helpers ----

    /**
     * Maps a logical object key to its physical HDFS relative path using a bucketed, flattened layout:
     * {@code data/<bucket>/<flattened-key>}. AutoMQ's key generator prefixes keys with a reversed-hex hash for S3
     * anti-hotspotting; on S3 those prefixes are virtual, but on HDFS every prefix segment is a real NameNode
     * directory inode, so the S3 layout would create ~1 directory per object and quickly exhaust the HDFS namespace
     * quota. Here the whole key is flattened into a single filename under a bounded set of {@value #KEY_SEP}-free
     * bucket directories, so the directory count stays constant regardless of object count. The logical key is
     * unchanged; {@link #listRecursive} reverses the mapping by decoding the filename.
     */
    private String dataRel(String key) {
        return DATA_DIR + "/" + bucketOf(key) + "/" + encodeKey(key);
    }

    /** Bounded bucket (256) derived from the key hash, so the number of directories is independent of object count. */
    private static String bucketOf(String key) {
        return String.format("%02x", key.hashCode() & 0xFF);
    }

    /** Flatten a logical key into a single HDFS filename ('/' -&gt; {@link #KEY_SEP}); reversed by {@link #decodeKey}. */
    private static String encodeKey(String key) {
        return key.replace('/', KEY_SEP);
    }

    private static String decodeKey(String name) {
        return name.replace(KEY_SEP, '/');
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
        // 3-tier resolution (static token -> Azure Workload Identity -> AAD_TOKEN env) lives in the shared resolver;
        // here we only extract the WebHDFS-specific config keys. The env HDFS_TOKEN_SCOPE fallback is handled inside.
        String token = bucketURI.extensionString("token", null);
        String scope = bucketURI.extensionString("tokenScope", null);
        return WorkloadIdentityTokenUtil.tokenSupplier(token, scope, "WebHDFS token");
    }

    private static String trimTrailingSlash(String s) {
        return s != null && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
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
