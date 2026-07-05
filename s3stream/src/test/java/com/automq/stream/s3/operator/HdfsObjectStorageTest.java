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
import com.automq.stream.utils.FutureUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Self-contained unit test for {@link HdfsObjectStorage} that runs against an in-process WebHDFS emulator
 * ({@link WebHdfsEmulator}) built on the JDK {@link HttpServer}. It needs no external gateway and locks in the
 * WebHDFS protocol behaviour the implementation depends on:
 * <ul>
 *     <li>two-step CREATE/APPEND handshake with {@code noredirect=true} (NameNode returns a DataNode Location,
 *     the payload is streamed in a second request);</li>
 *     <li>{@code MKDIRS} of the destination parent before {@code RENAME} (RENAME does not auto-create parents);</li>
 *     <li>range read, recursive list, delete, and multipart assembly / ranged part copy;</li>
 *     <li>404 mapped to {@link ObjectNotExistException} and the retry-strategy classification.</li>
 * </ul>
 */
@Tag("S3Unit")
public class HdfsObjectStorageTest {

    private WebHdfsEmulator emulator;
    private HdfsObjectStorage storage;

    @BeforeEach
    public void setup() throws IOException {
        emulator = new WebHdfsEmulator();
        emulator.start();
        BucketURI uri = BucketURI.parse("-3@hdfs://automq?token=test-token");
        uri.endpoint(emulator.endpoint());
        storage = HdfsObjectStorage.builder()
            .bucket(uri)
            .threadPrefix("hdfs-ut")
            .build();
    }

    @AfterEach
    public void cleanup() {
        if (storage != null) {
            storage.close();
        }
        if (emulator != null) {
            emulator.stop();
        }
    }

    @Test
    public void readinessCheckSucceeds() {
        assertTrue(storage.readinessCheck());
    }

    @Test
    public void writeReadDeleteUsesOneShotWrite() throws Exception {
        String key = "rw/obj-100";
        storage.write(new ObjectStorage.WriteOptions(), key, buf("hello world")).get();
        // The single write must have gone through the one-shot data=true handshake (no NameNode round-trip).
        assertTrue(emulator.sawOneShotCreate);

        ByteBuf full = storage.rangeRead(readOpts(), key, 0, -1L).get();
        assertEquals("hello world", str(full));
        full.release();

        ByteBuf part = storage.rangeRead(readOpts(), key, 0, 5).get();
        assertEquals("hello", str(part));
        part.release();

        storage.delete(paths(key)).get();
        Throwable cause = null;
        try {
            storage.rangeRead(readOpts(), key, 0, -1L).get();
        } catch (Throwable t) {
            cause = FutureUtil.cause(t);
        }
        assertInstanceOf(ObjectNotExistException.class, cause);
    }

    @Test
    public void listReturnsKeysUnderPrefix() throws Exception {
        storage.write(new ObjectStorage.WriteOptions(), "abc/def/100", buf("v1")).get();
        storage.write(new ObjectStorage.WriteOptions(), "abc/def/101", buf("v2")).get();
        storage.write(new ObjectStorage.WriteOptions(), "abc/deg/102", buf("v3")).get();

        assertEquals(List.of("abc/def/100", "abc/def/101", "abc/deg/102"), listKeys(""));
        assertEquals(List.of("abc/def/100", "abc/def/101"), listKeys("abc/def"));
    }

    @Test
    public void multipartAssemblyCreatesDestinationParentBeforeRename() throws Exception {
        String key = "mpu/obj-200";
        ObjectStorage.WriteOptions wo = new ObjectStorage.WriteOptions();
        String uploadId = storage.doCreateMultipartUpload(wo, key).get();
        AbstractObjectStorage.ObjectStorageCompletedPart p1 =
            storage.doUploadPart(wo, key, uploadId, 1, buf("part1-")).get();
        AbstractObjectStorage.ObjectStorageCompletedPart p2 =
            storage.doUploadPart(wo, key, uploadId, 2, buf("part2")).get();
        storage.doCompleteMultipartUpload(wo, key, uploadId, List.of(p1, p2)).get();

        // RENAME on the emulator fails unless the destination parent dir was created via MKDIRS first.
        assertTrue(emulator.sawMkdirsBeforeRename);

        ByteBuf assembled = storage.rangeRead(readOpts(), key, 0, -1L).get();
        assertEquals("part1-part2", str(assembled));
        assembled.release();
    }

    @Test
    public void multipartUploadPartCopy() throws Exception {
        String srcKey = "copy/src";
        String dstKey = "copy/dst";
        storage.write(new ObjectStorage.WriteOptions(), srcKey, buf("0123456789")).get();

        ObjectStorage.WriteOptions wo = new ObjectStorage.WriteOptions();
        String uploadId = storage.doCreateMultipartUpload(wo, dstKey).get();
        AbstractObjectStorage.ObjectStorageCompletedPart copied =
            storage.doUploadPartCopy(wo, srcKey, dstKey, 2, 6, uploadId, 1).get();
        AbstractObjectStorage.ObjectStorageCompletedPart appended =
            storage.doUploadPart(wo, dstKey, uploadId, 2, buf("XY")).get();
        storage.doCompleteMultipartUpload(wo, dstKey, uploadId, List.of(copied, appended)).get();

        ByteBuf result = storage.rangeRead(readOpts(), dstKey, 0, -1L).get();
        assertEquals("2345XY", str(result));
        result.release();
    }

    @Test
    public void rangeReadMissingObjectThrowsObjectNotExist() {
        CompletionException ex = assertThrows(CompletionException.class,
            () -> storage.rangeRead(readOpts(), "missing/key", 0, -1L).join());
        assertInstanceOf(ObjectNotExistException.class, FutureUtil.cause(ex));
    }

    @Test
    public void retryStrategyClassification() {
        // 5xx and connectivity errors retry; non-throttling 4xx and semantic errors abort.
        assertEquals(RetryStrategy.RETRY,
            storage.toRetryStrategyAndCause(new HdfsObjectStorage.WebHdfsException(500, "boom"), null).getLeft());
        assertEquals(RetryStrategy.RETRY,
            storage.toRetryStrategyAndCause(new HdfsObjectStorage.WebHdfsException(429, "throttle"), null).getLeft());
        assertEquals(RetryStrategy.ABORT,
            storage.toRetryStrategyAndCause(new HdfsObjectStorage.WebHdfsException(400, "bad"), null).getLeft());
        assertEquals(RetryStrategy.ABORT,
            storage.toRetryStrategyAndCause(new ObjectNotExistException(), null).getLeft());
    }

    // ---- helpers ----

    private ObjectStorage.ReadOptions readOpts() {
        return new ObjectStorage.ReadOptions().bucket(storage.bucketId());
    }

    private List<String> listKeys(String prefix) throws Exception {
        return storage.list(prefix).get().stream()
            .map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList());
    }

    private List<ObjectStorage.ObjectPath> paths(String... keys) {
        List<ObjectStorage.ObjectPath> list = new ArrayList<>();
        for (String key : keys) {
            list.add(new ObjectStorage.ObjectInfo(storage.bucketId(), key, 0, 0));
        }
        return list;
    }

    private static ByteBuf buf(String s) {
        return Unpooled.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String str(ByteBuf buf) {
        return buf.toString(StandardCharsets.UTF_8);
    }

    /**
     * Minimal in-process WebHDFS gateway sufficient to exercise {@link HdfsObjectStorage}. Files and directories are
     * held in memory keyed by their HDFS absolute path. It intentionally mirrors two real-gateway behaviours: writes
     * require the {@code noredirect} two-step handshake, and {@code RENAME} fails if the destination parent directory
     * was not created via {@code MKDIRS} beforehand.
     */
    private static final class WebHdfsEmulator {
        private static final ObjectMapper JSON = new ObjectMapper();
        private static final String MARKER = "/webhdfs/v1";
        private static final String PATH_PREFIX = "/testcluster/automq";

        private final Object lock = new Object();
        private final TreeMap<String, byte[]> files = new TreeMap<>();
        private final java.util.Set<String> dirs = new java.util.HashSet<>();
        private HttpServer server;

        volatile boolean sawOneShotCreate;
        volatile boolean sawMkdirsBeforeRename;

        void start() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        void stop() {
            server.stop(0);
        }

        String endpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + MARKER + PATH_PREFIX;
        }

        private void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                String abs = path.substring(path.indexOf(MARKER) + MARKER.length());
                Map<String, String> q = parseQuery(exchange.getRequestURI().getRawQuery());
                String op = q.getOrDefault("op", "").toUpperCase(java.util.Locale.ROOT);
                byte[] body = exchange.getRequestBody().readAllBytes();
                boolean data = "true".equals(q.get("data"));

                switch (op) {
                    case "GETFILESTATUS":
                        respondStatus(exchange, abs);
                        break;
                    case "CREATE":
                        if (data) {
                            sawOneShotCreate = true;
                            synchronized (lock) {
                                files.put(abs, body);
                                addDirs(parentOf(abs));
                            }
                            respond(exchange, 201, "");
                        } else {
                            respond(exchange, 200, locationJson(exchange, path, "CREATE"));
                        }
                        break;
                    case "APPEND":
                        if (data) {
                            synchronized (lock) {
                                files.merge(abs, body, HdfsObjectStorageTest::concat);
                            }
                            respond(exchange, 200, "");
                        } else {
                            respond(exchange, 200, locationJson(exchange, path, "APPEND"));
                        }
                        break;
                    case "OPEN":
                        open(exchange, abs, q);
                        break;
                    case "MKDIRS":
                        synchronized (lock) {
                            addDirs(abs);
                        }
                        respond(exchange, 200, "{\"boolean\":true}");
                        break;
                    case "RENAME":
                        rename(exchange, abs, q.get("destination"));
                        break;
                    case "DELETE":
                        delete(exchange, abs, "true".equals(q.get("recursive")));
                        break;
                    case "LISTSTATUS":
                        respond(exchange, 200, listStatus(abs));
                        break;
                    default:
                        respond(exchange, 400, "");
                }
            } catch (Throwable t) {
                respond(exchange, 500, t.getMessage() == null ? "" : t.getMessage());
            }
        }

        private void respondStatus(HttpExchange exchange, String abs) throws IOException {
            synchronized (lock) {
                if (files.containsKey(abs) || dirs.contains(abs)) {
                    respond(exchange, 200, "{\"FileStatus\":{\"type\":\"FILE\",\"length\":0}}");
                } else {
                    respond(exchange, 404, "{\"RemoteException\":{\"exception\":\"FileNotFoundException\"}}");
                }
            }
        }

        private void open(HttpExchange exchange, String abs, Map<String, String> q) throws IOException {
            byte[] content;
            synchronized (lock) {
                content = files.get(abs);
            }
            if (content == null) {
                respond(exchange, 404, "{\"RemoteException\":{\"exception\":\"FileNotFoundException\"}}");
                return;
            }
            long offset = Long.parseLong(q.getOrDefault("offset", "0"));
            byte[] slice;
            if (q.containsKey("length")) {
                int len = (int) Math.min(Long.parseLong(q.get("length")), content.length - offset);
                slice = new byte[Math.max(0, len)];
                System.arraycopy(content, (int) offset, slice, 0, slice.length);
            } else {
                slice = new byte[(int) (content.length - offset)];
                System.arraycopy(content, (int) offset, slice, 0, slice.length);
            }
            respondBytes(exchange, 200, slice);
        }

        private void rename(HttpExchange exchange, String fromAbs, String destAbs) throws IOException {
            synchronized (lock) {
                String parent = parentOf(destAbs);
                if (parent != null && !dirs.contains(parent)) {
                    respond(exchange, 404,
                        "{\"RemoteException\":{\"exception\":\"FileNotFoundException\",\"message\":\"rename destination parent "
                            + parent + " not found.\"}}");
                    return;
                }
                sawMkdirsBeforeRename = true;
                byte[] content = files.remove(fromAbs);
                if (content == null) {
                    respond(exchange, 404, "{\"RemoteException\":{\"exception\":\"FileNotFoundException\"}}");
                    return;
                }
                files.put(destAbs, content);
                respond(exchange, 200, "{\"boolean\":true}");
            }
        }

        private void delete(HttpExchange exchange, String abs, boolean recursive) throws IOException {
            synchronized (lock) {
                if (recursive) {
                    files.keySet().removeIf(k -> k.equals(abs) || k.startsWith(abs + "/"));
                    dirs.removeIf(d -> d.equals(abs) || d.startsWith(abs + "/"));
                } else {
                    files.remove(abs);
                }
            }
            respond(exchange, 200, "{\"boolean\":true}");
        }

        private String listStatus(String dirAbs) throws IOException {
            String prefix = dirAbs.endsWith("/") ? dirAbs : dirAbs + "/";
            Map<String, Boolean> children = new TreeMap<>();
            Map<String, Long> sizes = new HashMap<>();
            synchronized (lock) {
                for (Map.Entry<String, byte[]> e : files.entrySet()) {
                    if (e.getKey().startsWith(prefix)) {
                        String rest = e.getKey().substring(prefix.length());
                        int slash = rest.indexOf('/');
                        if (slash < 0) {
                            children.put(rest, false);
                            sizes.put(rest, (long) e.getValue().length);
                        } else {
                            children.put(rest.substring(0, slash), true);
                        }
                    }
                }
                for (String d : dirs) {
                    if (d.startsWith(prefix)) {
                        String rest = d.substring(prefix.length());
                        int slash = rest.indexOf('/');
                        children.putIfAbsent(slash < 0 ? rest : rest.substring(0, slash), true);
                    }
                }
            }
            ObjectNode root = JSON.createObjectNode();
            ArrayNode arr = root.putObject("FileStatuses").putArray("FileStatus");
            for (Map.Entry<String, Boolean> c : children.entrySet()) {
                ObjectNode node = arr.addObject();
                node.put("pathSuffix", c.getKey());
                node.put("type", c.getValue() ? "DIRECTORY" : "FILE");
                node.put("length", sizes.getOrDefault(c.getKey(), 0L));
                node.put("modificationTime", 1L);
            }
            return JSON.writeValueAsString(root);
        }

        private String locationJson(HttpExchange exchange, String path, String op) {
            String base = "http://127.0.0.1:" + exchange.getLocalAddress().getPort();
            String location = base + path + "?op=" + op + "&data=true";
            return "{\"Location\":\"" + location + "\"}";
        }

        private void addDirs(String dirAbs) {
            String cur = dirAbs;
            while (cur != null && cur.startsWith(PATH_PREFIX) && dirs.add(cur)) {
                cur = parentOf(cur);
            }
        }

        private static String parentOf(String abs) {
            if (abs == null) {
                return null;
            }
            int idx = abs.lastIndexOf('/');
            return idx <= 0 ? null : abs.substring(0, idx);
        }

        private static Map<String, String> parseQuery(String rawQuery) {
            Map<String, String> map = new HashMap<>();
            if (rawQuery == null || rawQuery.isEmpty()) {
                return map;
            }
            for (String pair : rawQuery.split("&")) {
                int eq = pair.indexOf('=');
                if (eq < 0) {
                    map.put(pair, "");
                } else {
                    map.put(pair.substring(0, eq), pair.substring(eq + 1));
                }
            }
            return map;
        }

        private static void respond(HttpExchange exchange, int code, String body) throws IOException {
            respondBytes(exchange, code, body.getBytes(StandardCharsets.UTF_8));
        }

        private static void respondBytes(HttpExchange exchange, int code, byte[] body) throws IOException {
            if (body.length == 0) {
                exchange.sendResponseHeaders(code, -1);
            } else {
                exchange.sendResponseHeaders(code, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            }
            exchange.close();
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
