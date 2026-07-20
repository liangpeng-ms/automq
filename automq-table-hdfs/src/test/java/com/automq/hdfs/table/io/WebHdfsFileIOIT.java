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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live end-to-end test of {@link WebHdfsFileIO} against a real MT WebHDFS gateway (gateway derived from the
 * {@code hdfs://} NameNode authority). Disabled unless both {@code AAD_TOKEN} and {@code HDFS_TABLE_BASE} are set, so it
 * never runs in the normal CI selection.
 * <p>
 * Run with (PowerShell):
 * <pre>
 * $env:AAD_TOKEN = (az account get-access-token --resource api://1c0b8c88-563f-4b97-abdf-207172a50d2c --query accessToken -o tsv)
 * $env:HDFS_TABLE_BASE = "hdfs://namenode0-vipv4.MTPrime-PROD-DUBE01.DUBE01.ap.gbl/user/pelian/test/falcon"
 * ./gradlew :automq-table-hdfs:test --tests com.automq.hdfs.table.io.WebHdfsFileIOIT
 * </pre>
 */
@Timeout(180)
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "HDFS_TABLE_BASE", matches = ".+")
public class WebHdfsFileIOIT {
    private WebHdfsFileIO io;
    private String runDir;

    @BeforeEach
    public void setup() {
        io = new WebHdfsFileIO();
        // No webhdfs.token / gateway: token falls back to the AAD_TOKEN env var and the gateway is derived from the
        // hdfs:// authority of each location.
        io.initialize(Map.of());
        String base = stripTrailingSlash(System.getenv("HDFS_TABLE_BASE"));
        // Unique subdir isolates this run and enables precise cleanup on the shared gateway.
        runDir = base + "/webhdfs-fileio-it/" + UUID.randomUUID();
    }

    @AfterEach
    public void cleanup() {
        if (io != null) {
            io.close();
        }
    }

    @Test
    public void writeReadDeleteAgainstRealGateway() throws IOException {
        String loc = runDir + "/data/00000-0-file.bin";
        byte[] payload = ("webhdfs-fileio-it " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);

        OutputFile out = io.newOutputFile(loc);
        try (PositionOutputStream os = out.createOrOverwrite()) {
            os.write(payload);
            assertEquals(payload.length, os.getPos());
        }

        InputFile in = io.newInputFile(loc);
        assertTrue(in.exists(), "file should exist after write");
        assertEquals(payload.length, in.getLength());
        try (SeekableInputStream is = in.newStream()) {
            assertArrayEquals(payload, is.readAllBytes());
            // seek to an arbitrary offset and read the tail
            is.seek(payload.length - 4);
            byte[] tail = is.readAllBytes();
            byte[] expectedTail = new byte[4];
            System.arraycopy(payload, payload.length - 4, expectedTail, 0, 4);
            assertArrayEquals(expectedTail, tail);
        }

        io.deleteFile(loc);
        assertFalse(io.newInputFile(loc).exists(), "file should be gone after delete");
    }

    @Test
    public void parquetRoundTripAgainstRealGateway() throws IOException {
        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "name", Types.StringType.get()));
        String loc = runDir + "/data/00001-0-parquet.parquet";

        List<Record> expected = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
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

    private static String stripTrailingSlash(String s) {
        String v = s;
        while (v.endsWith("/")) {
            v = v.substring(0, v.length() - 1);
        }
        return v;
    }
}
