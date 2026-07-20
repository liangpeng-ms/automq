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

package com.automq.hdfs.table.io;

import com.automq.stream.s3.webhdfs.WebHdfsClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Self-contained test for {@link WebHdfsFileIO} against an in-process WebHDFS emulator (JDK {@link HttpServer}); no
 * external gateway or Hadoop dependency. Verifies the Iceberg FileIO contract the Parquet writer/reader relies on:
 * one-shot CREATE, ranged/seeked OPEN, GETFILESTATUS length + exists, and DELETE.
 */
public class WebHdfsFileIOTest {

    private Emulator emulator;
    private WebHdfsFileIO io;
    private String base;

    @BeforeEach
    public void setup() throws IOException {
        emulator = new Emulator();
        emulator.start();
        io = new WebHdfsFileIO();
        io.initialize(Map.of(
            WebHdfsFileIO.TOKEN_PROP, "test-token",
            WebHdfsFileIO.GATEWAY_PROP, "http://127.0.0.1:" + emulator.port() + "/webhdfs/v1"));
        base = "hdfs://namenode/user/pelian/iceberg";
    }

    @AfterEach
    public void cleanup() {
        io.close();
        emulator.stop();
    }

    @Test
    public void writeReadDeleteRoundTrip() throws IOException {
        String loc = base + "/db/tbl/data/00000-0-file.parquet";
        byte[] payload = "hello-webhdfs-fileio".getBytes(StandardCharsets.UTF_8);

        OutputFile out = io.newOutputFile(loc);
        try (PositionOutputStream os = out.createOrOverwrite()) {
            os.write(payload);
            assertEquals(payload.length, os.getPos());
        }

        InputFile in = io.newInputFile(loc);
        assertTrue(in.exists());
        assertEquals(payload.length, in.getLength());
        try (SeekableInputStream is = in.newStream()) {
            byte[] read = is.readAllBytes();
            assertArrayEquals(payload, read);
        }

        io.deleteFile(loc);
        assertFalse(io.newInputFile(loc).exists());
    }

    @Test
    public void seekReadsFromArbitraryOffsets() throws IOException {
        String loc = base + "/db/tbl/data/00001-0-file.parquet";
        // 10000 deterministic bytes: value[i] = i % 251
        byte[] payload = new byte[10000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        try (PositionOutputStream os = io.newOutputFile(loc).createOrOverwrite()) {
            os.write(payload);
        }

        InputFile in = io.newInputFile(loc);
        try (SeekableInputStream is = in.newStream()) {
            // Parquet-like access: read a chunk in the middle, then jump near EOF (footer), then back to start.
            is.seek(4096);
            byte[] mid = new byte[256];
            readFully(is, mid);
            assertEquals(4096 + 256, is.getPos());
            assertRange(payload, 4096, mid);

            is.seek(payload.length - 8);
            byte[] tail = new byte[8];
            readFully(is, tail);
            assertRange(payload, payload.length - 8, tail);

            is.seek(0);
            byte[] head = new byte[16];
            readFully(is, head);
            assertRange(payload, 0, head);
        }
    }

    private static void assertRange(byte[] full, int offset, byte[] actual) {
        byte[] expected = new byte[actual.length];
        System.arraycopy(full, offset, expected, 0, actual.length);
        assertArrayEquals(expected, actual, "mismatch at offset " + offset);
    }

    /**
     * End-to-end proof that the real Iceberg Parquet writer and reader work through {@link WebHdfsFileIO}: write
     * records with {@link Parquet#write} (which drives {@code createOrOverwrite}, {@code getPos}, {@code close}), then
     * read them back with {@link Parquet#read} (which drives {@code getLength}, {@code newStream}, footer seeks).
     */
    @Test
    public void parquetWriteReadRoundTrip() throws IOException {
        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "name", Types.StringType.get()));
        String loc = base + "/db/tbl/data/00002-0-parquet.parquet";

        List<Record> expected = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            GenericRecord r = GenericRecord.create(schema);
            r.setField("id", (long) i);
            r.setField("name", "row-" + i);
            expected.add(r);
        }

        OutputFile out = io.newOutputFile(loc);
        try (FileAppender<Record> appender = Parquet.write(out)
            .schema(schema)
            .createWriterFunc(msgType -> GenericParquetWriter.create(schema, msgType))
            .overwrite()
            .build()) {
            appender.addAll(expected);
        }

        InputFile in = io.newInputFile(loc);
        assertTrue(in.exists());
        assertTrue(in.getLength() > 0);

        List<Record> actual = new ArrayList<>();
        try (CloseableIterable<Record> reader = Parquet.read(in)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
            for (Record r : reader) {
                actual.add(r);
            }
        }

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getField("id"), actual.get(i).getField("id"), "id at row " + i);
            assertEquals(expected.get(i).getField("name"), actual.get(i).getField("name"), "name at row " + i);
        }

        io.deleteFile(loc);
        assertFalse(io.newInputFile(loc).exists());
    }

    /**
     * Unity Catalog stores table locations with the HDFS RPC scheme ({@code hdfs://<namenode>/abs/path}), whose
     * authority is the NameNode rather than the WebHDFS gateway. Verify that with {@code webhdfs.gateway} configured
     * (including an extra-NameNode mount prefix), such a location is rewritten to {@code gateway + absPath} and
     * round-trips. Mirrors the confirmed mapping
     * hdfs://namenode2extra7-vipv4.MTPrime-PROD-MWHE02.MWHE02.ap.gbl/user/pelian/test/automq/data/data &rarr;
     * https://hdfs-http-ipv4-mtprime-mwhe02-2:83/webhdfs/v1/MTPrime-MWHE02-2-Extra-7/user/pelian/test/automq/data/data.
     */
    @Test
    public void hdfsSchemeLocationViaGateway() throws IOException {
        String gateway = "http://127.0.0.1:" + emulator.port() + "/webhdfs/v1/MTPrime-MWHE02-2-Extra-7";
        WebHdfsFileIO hdfsIo = new WebHdfsFileIO();
        hdfsIo.initialize(Map.of(
            WebHdfsFileIO.TOKEN_PROP, "test-token",
            WebHdfsFileIO.GATEWAY_PROP, gateway));
        try {
            String loc = "hdfs://namenode2extra7-vipv4.MTPrime-PROD-MWHE02.MWHE02.ap.gbl/user/pelian/test/automq/data/data/00003-0.parquet";
            byte[] payload = "hdfs-scheme-via-gateway".getBytes(StandardCharsets.UTF_8);

            try (PositionOutputStream os = hdfsIo.newOutputFile(loc).createOrOverwrite()) {
                os.write(payload);
            }

            InputFile in = hdfsIo.newInputFile(loc);
            assertTrue(in.exists());
            assertEquals(payload.length, in.getLength());
            try (SeekableInputStream is = in.newStream()) {
                assertArrayEquals(payload, is.readAllBytes());
            }

            hdfsIo.deleteFile(loc);
            assertFalse(hdfsIo.newInputFile(loc).exists());
        } finally {
            hdfsIo.close();
        }
    }

    /**
     * By default (no {@code webhdfs.gateway} override) the MT WebHDFS HTTP v2 gateway is derived from the location's
     * {@code hdfs://} NameNode authority. Verify the two confirmed mappings, including the extra-NameNode form.
     */
    @Test
    public void derivesGatewayFromHdfsAuthority() {
        assertEquals(
            "https://hdfs-http-ipv4-mtprime-dube01-0.magnetar.binginternal.com:83/webhdfs/v1/MTPrime-DUBE01-0",
            WebHdfsClient.deriveGateway("namenode0-vipv4.MTPrime-PROD-DUBE01.DUBE01.ap.gbl"));
        assertEquals(
            "https://hdfs-http-ipv4-mtprime-mwhe02-2.magnetar.binginternal.com:83/webhdfs/v1/MTPrime-MWHE02-2-Extra-7",
            WebHdfsClient.deriveGateway("namenode2extra7-vipv4.MTPrime-PROD-MWHE02.MWHE02.ap.gbl"));

        WebHdfsClient client = new WebHdfsClient(() -> "t", java.time.Duration.ofSeconds(1));
        assertEquals(
            "https://hdfs-http-ipv4-mtprime-mwhe02-2.magnetar.binginternal.com:83/webhdfs/v1/MTPrime-MWHE02-2-Extra-7/user/pelian/test/automq/data/data",
            client.restUrl("hdfs://namenode2extra7-vipv4.MTPrime-PROD-MWHE02.MWHE02.ap.gbl/user/pelian/test/automq/data/data"));
    }

    private static void readFully(InputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = in.read(buf, off, buf.length - off);
            if (n < 0) {
                throw new IOException("unexpected EOF at " + off);
            }
            off += n;
        }
    }

    /** Minimal in-process WebHDFS gateway: CREATE (one-shot data=true), OPEN (offset/length), GETFILESTATUS, MKDIRS, DELETE. */
    private static final class Emulator {
        private static final String MARKER = "/webhdfs/v1";
        private final Object lock = new Object();
        private final TreeMap<String, byte[]> files = new TreeMap<>();
        private HttpServer server;

        void start() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        void stop() {
            server.stop(0);
        }

        int port() {
            return server.getAddress().getPort();
        }

        private void handle(HttpExchange ex) throws IOException {
            try {
                String path = ex.getRequestURI().getPath();
                String abs = path.substring(path.indexOf(MARKER) + MARKER.length());
                Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
                String op = q.getOrDefault("op", "").toUpperCase(Locale.ROOT);
                byte[] body = ex.getRequestBody().readAllBytes();
                switch (op) {
                    case "CREATE":
                        synchronized (lock) {
                            files.put(abs, body);
                        }
                        respond(ex, 201, new byte[0]);
                        break;
                    case "RENAME": {
                        String dest = q.get("destination");
                        synchronized (lock) {
                            byte[] c = files.remove(abs);
                            if (c == null) {
                                respond(ex, 404, new byte[0]);
                                break;
                            }
                            files.put(dest, c);
                        }
                        respond(ex, 200, "{\"boolean\":true}".getBytes(StandardCharsets.UTF_8));
                        break;
                    }
                    case "MKDIRS":
                        respond(ex, 200, "{\"boolean\":true}".getBytes(StandardCharsets.UTF_8));
                        break;
                    case "DELETE":
                        synchronized (lock) {
                            files.remove(abs);
                        }
                        respond(ex, 200, "{\"boolean\":true}".getBytes(StandardCharsets.UTF_8));
                        break;
                    case "GETFILESTATUS": {
                        byte[] c;
                        synchronized (lock) {
                            c = files.get(abs);
                        }
                        if (c == null) {
                            respond(ex, 404, "{\"RemoteException\":{\"exception\":\"FileNotFoundException\"}}".getBytes(StandardCharsets.UTF_8));
                        } else {
                            respond(ex, 200, ("{\"FileStatus\":{\"type\":\"FILE\",\"length\":" + c.length + "}}").getBytes(StandardCharsets.UTF_8));
                        }
                        break;
                    }
                    case "OPEN": {
                        byte[] c;
                        synchronized (lock) {
                            c = files.get(abs);
                        }
                        if (c == null) {
                            respond(ex, 404, new byte[0]);
                            break;
                        }
                        long offset = Long.parseLong(q.getOrDefault("offset", "0"));
                        int start = (int) Math.min(offset, c.length);
                        int len = c.length - start;
                        if (q.containsKey("length")) {
                            len = (int) Math.min(Long.parseLong(q.get("length")), len);
                        }
                        byte[] slice = new byte[Math.max(0, len)];
                        System.arraycopy(c, start, slice, 0, slice.length);
                        respond(ex, 200, slice);
                        break;
                    }
                    default:
                        respond(ex, 400, new byte[0]);
                }
            } catch (Throwable t) {
                respond(ex, 500, (t.getMessage() == null ? "" : t.getMessage()).getBytes(StandardCharsets.UTF_8));
            }
        }

        private static Map<String, String> parseQuery(String raw) {
            Map<String, String> map = new HashMap<>();
            if (raw == null || raw.isEmpty()) {
                return map;
            }
            for (String pair : raw.split("&")) {
                int eq = pair.indexOf('=');
                if (eq < 0) {
                    map.put(pair, "");
                } else {
                    map.put(pair.substring(0, eq), pair.substring(eq + 1));
                }
            }
            return map;
        }

        private static void respond(HttpExchange ex, int code, byte[] body) throws IOException {
            if (body.length == 0) {
                ex.sendResponseHeaders(code, -1);
            } else {
                ex.sendResponseHeaders(code, body.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(body);
                }
            }
            ex.close();
        }
    }
}
