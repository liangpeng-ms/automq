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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live integration test that exercises {@link HdfsObjectStorage} against a real WebHDFS gateway.
 * <p>
 * It is disabled unless both environment variables are present, so it never runs in the normal CI selection:
 * <ul>
 *     <li>{@code HDFS_WAL_ENDPOINT} - full WebHDFS base URL, e.g.
 *     {@code https://host:83/webhdfs/v1/<subcluster>/user/<name>/test/automq}</li>
 *     <li>{@code AAD_TOKEN} - Entra ID bearer token used for authentication.</li>
 * </ul>
 * Run with:
 * <pre>
 * $env:HDFS_WAL_ENDPOINT = "https://.../webhdfs/v1/.../automq"
 * $env:AAD_TOKEN = "&lt;bearer token&gt;"
 * ./gradlew :s3stream:test --tests com.automq.stream.s3.operator.HdfsObjectStorageIT
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "HDFS_WAL_ENDPOINT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
public class HdfsObjectStorageIT {
    private static final long TIMEOUT_MS = 60_000;

    private HdfsObjectStorage storage;
    private String prefix;

    @BeforeEach
    public void setup() {
        BucketURI uri = BucketURI.parse("-3@hdfs://automq");
        uri.endpoint(System.getenv("HDFS_WAL_ENDPOINT"));
        storage = HdfsObjectStorage.builder()
            .bucket(uri)
            .threadPrefix("hdfs-it")
            .build();
        // Unique prefix per run so parallel/repeated runs never collide on the shared gateway.
        prefix = "it-" + UUID.randomUUID() + "/";
    }

    @AfterEach
    public void cleanup() {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    public void readinessCheckSucceeds() {
        assertTrue(storage.readinessCheck(), "WebHDFS readiness check should succeed against a reachable gateway");
    }

    @Test
    public void writeReadDelete() throws Exception {
        String key = prefix + "rw/obj-100";

        storage.write(new ObjectStorage.WriteOptions(), key, buf("hello world")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        ByteBuf full = storage.rangeRead(readOpts(), key, 0, -1L).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals("hello world", str(full));
        full.release();

        ByteBuf part = storage.rangeRead(readOpts(), key, 0, 5).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals("hello", str(part));
        part.release();

        deleteKeys(key);

        Throwable cause = null;
        try {
            storage.rangeRead(readOpts(), key, 0, -1L).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            cause = FutureUtil.cause(t);
        }
        assertInstanceOf(ObjectNotExistException.class, cause, "reading a deleted object should fail with ObjectNotExistException");
    }

    @Test
    public void listReturnsKeysUnderPrefix() throws Exception {
        String k1 = prefix + "abc/def/100";
        String k2 = prefix + "abc/def/101";
        String k3 = prefix + "abc/deg/102";
        storage.write(new ObjectStorage.WriteOptions(), k1, buf("v1")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        storage.write(new ObjectStorage.WriteOptions(), k2, buf("v2")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        storage.write(new ObjectStorage.WriteOptions(), k3, buf("v3")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        List<String> all = storage.list(prefix).get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList());
        assertEquals(List.of(k1, k2, k3), all);

        List<String> underDef = storage.list(prefix + "abc/def").get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .stream().map(ObjectStorage.ObjectPath::key).sorted().collect(Collectors.toList());
        assertEquals(List.of(k1, k2), underDef);

        deleteKeys(k1, k2, k3);
    }

    @Test
    public void multipartUploadAssemblesParts() throws Exception {
        String key = prefix + "mpu/obj-200";
        ObjectStorage.WriteOptions wo = new ObjectStorage.WriteOptions();

        String uploadId = storage.doCreateMultipartUpload(wo, key).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        AbstractObjectStorage.ObjectStorageCompletedPart p1 =
            storage.doUploadPart(wo, key, uploadId, 1, buf("part1-")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        AbstractObjectStorage.ObjectStorageCompletedPart p2 =
            storage.doUploadPart(wo, key, uploadId, 2, buf("part2")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        storage.doCompleteMultipartUpload(wo, key, uploadId, List.of(p1, p2)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        ByteBuf assembled = storage.rangeRead(readOpts(), key, 0, -1L).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals("part1-part2", str(assembled));
        assembled.release();

        deleteKeys(key);
    }

    @Test
    public void multipartUploadPartCopy() throws Exception {
        String srcKey = prefix + "copy/src";
        String dstKey = prefix + "copy/dst";
        storage.write(new ObjectStorage.WriteOptions(), srcKey, buf("0123456789")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        ObjectStorage.WriteOptions wo = new ObjectStorage.WriteOptions();
        String uploadId = storage.doCreateMultipartUpload(wo, dstKey).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        // Copy the range [2, 6) = "2345" from the source object into part 1 without downloading the whole object.
        AbstractObjectStorage.ObjectStorageCompletedPart copied =
            storage.doUploadPartCopy(wo, srcKey, dstKey, 2, 6, uploadId, 1).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        AbstractObjectStorage.ObjectStorageCompletedPart appended =
            storage.doUploadPart(wo, dstKey, uploadId, 2, buf("XY")).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        storage.doCompleteMultipartUpload(wo, dstKey, uploadId, List.of(copied, appended)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        ByteBuf result = storage.rangeRead(readOpts(), dstKey, 0, -1L).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals("2345XY", str(result));
        result.release();

        deleteKeys(srcKey, dstKey);
    }

    private void deleteKeys(String... keys) throws Exception {
        List<ObjectStorage.ObjectPath> paths = new ArrayList<>();
        for (String key : keys) {
            paths.add(new ObjectStorage.ObjectInfo(storage.bucketId(), key, 0, 0));
        }
        storage.delete(paths).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private ObjectStorage.ReadOptions readOpts() {
        return new ObjectStorage.ReadOptions().bucket(storage.bucketId());
    }

    private static ByteBuf buf(String s) {
        return Unpooled.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String str(ByteBuf buf) {
        return buf.toString(StandardCharsets.UTF_8);
    }
}
