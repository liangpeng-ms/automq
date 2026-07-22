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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.HdfsObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live end-to-end test of the object WAL ({@link ObjectWALService}) running on top of {@link HdfsObjectStorage}
 * against a real WebHDFS gateway. Disabled unless both {@code HDFS_WAL_ENDPOINT} and {@code AAD_TOKEN} are set, so it
 * never runs in the normal CI selection.
 * <p>
 * Run with:
 * <pre>
 * $env:HDFS_WAL_ENDPOINT = "https://.../webhdfs/v1/.../automq"
 * $env:AAD_TOKEN = "&lt;bearer token&gt;"
 * ./gradlew :s3stream:test --tests com.automq.stream.s3.wal.impl.object.HdfsWalIT
 * </pre>
 */
@Timeout(180)
@EnabledIfEnvironmentVariable(named = "HDFS_WAL_ENDPOINT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
public class HdfsWalIT {
    private static final long STREAM_ID = 233L;
    private static final int NODE_ID = 900;

    private final Time time = Time.SYSTEM;
    private final Random random = new Random();
    private HdfsObjectStorage objectStorage;
    private String clusterId;

    @BeforeEach
    public void setup() {
        BucketURI uri = BucketURI.parse("-3@hdfs://automq");
        uri.endpoint(System.getenv("HDFS_WAL_ENDPOINT"));
        objectStorage = HdfsObjectStorage.builder()
            .bucket(uri)
            .threadPrefix("hdfs-wal-it")
            .build();
        // Unique cluster id isolates this run's WAL objects on the shared gateway and enables precise cleanup.
        clusterId = "it-wal-" + UUID.randomUUID();
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (objectStorage != null) {
            try {
                List<ObjectStorage.ObjectPath> objects = new ArrayList<>(
                    objectStorage.list(ObjectUtils.nodePrefix(clusterId, NODE_ID)).get());
                if (!objects.isEmpty()) {
                    objectStorage.delete(objects).get();
                }
            } finally {
                objectStorage.close();
            }
        }
    }

    @Test
    public void appendThenGetReturnsRecords() throws Exception {
        ObjectWALConfig config = config(0L);
        ObjectWALService wal = new ObjectWALService(time, objectStorage, config);
        acquire(0L);
        wal.start();

        List<AppendResult> appended = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            appended.add(wal.append(TraceContext.DEFAULT,
                StreamRecordBatch.of(STREAM_ID, 10, 100L + i, 1, randomBuf(256), DefaultByteBufSupplier.INSTANCE)).get());
        }
        ((DefaultWriter) wal.writer).flush().join();

        for (int i = 0; i < appended.size(); i++) {
            StreamRecordBatch record = wal.get(appended.get(i).recordOffset()).get();
            assertEquals(STREAM_ID, record.getStreamId());
            assertEquals(100L + i, record.getBaseOffset());
            record.release();
        }
        wal.shutdownGracefully();
    }

    @Test
    public void recoverAfterTrimAcrossEpoch() throws Exception {
        // Epoch 0: append 10 records, flush, then trim through the 5th record (baseOffset 104).
        ObjectWALConfig epoch0 = config(0L);
        ObjectWALService wal0 = new ObjectWALService(time, objectStorage, epoch0);
        acquire(0L);
        wal0.start();
        assertTrue(recoverAll(wal0).isEmpty(), "a fresh WAL should recover no records");

        List<AppendResult> appended = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            appended.add(wal0.append(TraceContext.DEFAULT,
                StreamRecordBatch.of(STREAM_ID, 10, 100L + i, 1, randomBuf(256), DefaultByteBufSupplier.INSTANCE)).get());
        }
        ((DefaultWriter) wal0.writer).flush().join();
        wal0.trim(appended.get(4).recordOffset()).get();
        wal0.shutdownGracefully();

        // Epoch 1: reopen and recover; only the records after the trim offset (baseOffset 105..109) should remain.
        ObjectWALConfig epoch1 = config(1L);
        ObjectWALService wal1 = new ObjectWALService(time, objectStorage, epoch1);
        acquire(1L);
        wal1.start();

        List<RecoverResult> recovered = recoverAll(wal1);
        assertEquals(5, recovered.size());
        for (int i = 0; i < recovered.size(); i++) {
            assertEquals(105L + i, recovered.get(i).record().getBaseOffset());
        }
        wal1.shutdownGracefully();
    }

    private ObjectWALConfig config(long epoch) {
        return ObjectWALConfig.builder()
            .withClusterId(clusterId)
            .withNodeId(NODE_ID)
            .withEpoch(epoch)
            .withBucketId(objectStorage.bucketId())
            .withMaxBytesInBatch(1024)
            .withBatchInterval(100)
            .withReservationService(new ObjectReservationService(clusterId, objectStorage, objectStorage.bucketId()))
            .build();
    }

    private void acquire(long epoch) {
        new ObjectReservationService(clusterId, objectStorage, objectStorage.bucketId())
            .acquire(NODE_ID, epoch, false)
            .join();
    }

    private static List<RecoverResult> recoverAll(ObjectWALService wal) {
        List<RecoverResult> records = new ArrayList<>();
        wal.recover().forEachRemaining(records::add);
        return records;
    }

    private ByteBuf randomBuf(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return Unpooled.wrappedBuffer(bytes);
    }
}
