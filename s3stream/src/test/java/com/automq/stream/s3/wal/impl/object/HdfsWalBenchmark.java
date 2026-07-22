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
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Performance benchmark for {@link HdfsObjectStorage} and the object WAL running on it, used to evaluate whether a
 * WebHDFS gateway is a viable low-latency WAL backend compared with S3. It measures:
 * <ol>
 *     <li>raw object-storage write latency and throughput across payload sizes;</li>
 *     <li>raw range-read latency;</li>
 *     <li>WAL append commit latency (P50/P99/...) and sustained throughput through {@link ObjectWALService}.</li>
 * </ol>
 * Results are printed to stdout. It is opt-in only: it runs when {@code HDFS_BENCH=true} and both
 * {@code HDFS_WAL_ENDPOINT} and {@code AAD_TOKEN} are set, so it never runs in CI or alongside the functional tests.
 * <pre>
 * $env:HDFS_WAL_ENDPOINT = "https://.../webhdfs/v1/.../automq"
 * $env:AAD_TOKEN = "&lt;bearer token&gt;"
 * $env:HDFS_BENCH = "true"
 * ./gradlew :s3stream:test --tests com.automq.stream.s3.wal.impl.object.HdfsWalBenchmark -x spotbugsMain -x spotbugsTest
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "HDFS_BENCH", matches = "true")
@EnabledIfEnvironmentVariable(named = "HDFS_WAL_ENDPOINT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
public class HdfsWalBenchmark {
    private static final int NODE_ID = 950;

    private final Time time = Time.SYSTEM;
    private final Random random = new Random(42);
    private final StringBuilder report = new StringBuilder();
    private HdfsObjectStorage storage;
    private String clusterId;
    private String benchPrefix;

    @BeforeEach
    public void setup() {
        BucketURI uri = BucketURI.parse("-3@hdfs://automq");
        uri.endpoint(System.getenv("HDFS_WAL_ENDPOINT"));
        storage = HdfsObjectStorage.builder().bucket(uri).threadPrefix("hdfs-bench").build();
        clusterId = "bench-wal-" + UUID.randomUUID();
        benchPrefix = "bench-obj-" + UUID.randomUUID() + "/";
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (report.length() > 0) {
            Path out = Path.of(System.getProperty("user.dir"), "hdfs-bench-results.txt");
            Files.writeString(out, report.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (storage == null) {
            return;
        }
        try {
            deleteByPrefix(benchPrefix);
            deleteByPrefix(ObjectUtils.nodePrefix(clusterId, NODE_ID));
        } finally {
            storage.close();
        }
    }

    @Test
    public void rawObjectStorageWriteLatency() throws Exception {
        int iterations = envInt("BENCH_RAW_ITERS", 30);
        int[] sizes = {4 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024};
        System.out.println("\n=== Raw object-storage WRITE latency (" + iterations + " iters/size) ===");
        log("=== Raw object-storage WRITE latency (" + iterations + " iters/size) ===");
        log(header());
        for (int size : sizes) {
            long[] latencies = new long[iterations];
            byte[] payload = new byte[size];
            random.nextBytes(payload);
            // Warmup once so the connection/TLS handshake is not counted.
            storage.write(new ObjectStorage.WriteOptions(), benchPrefix + "warmup/" + size,
                Unpooled.wrappedBuffer(payload)).get(60, TimeUnit.SECONDS);
            for (int i = 0; i < iterations; i++) {
                String key = benchPrefix + "w/" + size + "/" + i;
                long start = System.nanoTime();
                storage.write(new ObjectStorage.WriteOptions(), key, Unpooled.wrappedBuffer(payload))
                    .get(60, TimeUnit.SECONDS);
                latencies[i] = System.nanoTime() - start;
            }
            printStats(humanSize(size), latencies, (long) size * iterations);
        }
    }

    @Test
    public void rawObjectStorageReadLatency() throws Exception {
        int iterations = envInt("BENCH_RAW_ITERS", 50);
        int size = envInt("BENCH_READ_SIZE", 1024 * 1024);
        String key = benchPrefix + "read/obj";
        byte[] payload = new byte[size];
        random.nextBytes(payload);
        storage.write(new ObjectStorage.WriteOptions(), key, Unpooled.wrappedBuffer(payload)).get(60, TimeUnit.SECONDS);

        long[] latencies = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            ByteBuf buf = storage.rangeRead(new ObjectStorage.ReadOptions().bucket(storage.bucketId()), key, 0, -1L)
                .get(60, TimeUnit.SECONDS);
            latencies[i] = System.nanoTime() - start;
            buf.release();
        }
        System.out.println("\n=== Raw object-storage READ latency (" + humanSize(size) + ", " + iterations + " iters) ===");
        log("=== Raw object-storage READ latency (" + humanSize(size) + ", " + iterations + " iters) ===");
        log(header());
        printStats(humanSize(size), latencies, (long) size * iterations);
    }

    @Test
    public void walAppendLatencyAndThroughput() throws Exception {
        int records = envInt("BENCH_WAL_RECORDS", 2000);
        int recordSize = envInt("BENCH_WAL_RECORD_SIZE", 1024);
        long maxBytesInBatch = envInt("BENCH_WAL_BATCH_BYTES", 1024 * 1024);
        long batchIntervalMs = envInt("BENCH_WAL_BATCH_INTERVAL_MS", 10);

        ObjectWALConfig config = ObjectWALConfig.builder()
            .withClusterId(clusterId)
            .withNodeId(NODE_ID)
            .withEpoch(0L)
            .withBucketId(storage.bucketId())
            .withMaxBytesInBatch(maxBytesInBatch)
            .withBatchInterval(batchIntervalMs)
            .withReservationService(new ObjectReservationService(clusterId, storage, storage.bucketId()))
            .build();
        new ObjectReservationService(clusterId, storage, storage.bucketId()).acquire(NODE_ID, 0L, false).join();
        ObjectWALService wal = new ObjectWALService(time, storage, config);
        wal.start();

        // Warmup a few appends.
        List<CompletableFuture<AppendResult>> warmup = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            warmup.add(wal.append(TraceContext.DEFAULT, StreamRecordBatch.of(1L, 0, i, 1, payload(recordSize), DefaultByteBufSupplier.INSTANCE)));
        }
        CompletableFuture.allOf(warmup.toArray(new CompletableFuture[0])).get(120, TimeUnit.SECONDS);

        long[] latencies = new long[records];
        List<CompletableFuture<AppendResult>> futures = new ArrayList<>(records);
        long benchStart = System.nanoTime();
        for (int i = 0; i < records; i++) {
            final int idx = i;
            long submit = System.nanoTime();
            CompletableFuture<AppendResult> cf = wal.append(TraceContext.DEFAULT,
                StreamRecordBatch.of(1L, 0, 100L + i, 1, payload(recordSize), DefaultByteBufSupplier.INSTANCE));
            cf.whenComplete((r, e) -> latencies[idx] = System.nanoTime() - submit);
            futures.add(cf);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(300, TimeUnit.SECONDS);
        long wallNanos = System.nanoTime() - benchStart;

        System.out.println("\n=== WAL append commit latency & throughput ===");
        log("=== WAL append commit latency & throughput ===");
        log(String.format("records=%d recordSize=%s maxBytesInBatch=%s batchInterval=%dms",
            records, humanSize(recordSize), humanSize((int) maxBytesInBatch), batchIntervalMs));
        log(header());
        printStats("wal-append", latencies, (long) records * recordSize, wallNanos);

        wal.shutdownGracefully();
    }

    @Test
    public void rawObjectStorageWriteThroughputConcurrent() throws Exception {
        int count = envInt("BENCH_CONC_COUNT", 200);
        int size = envInt("BENCH_CONC_SIZE", 1024 * 1024);
        int concurrency = envInt("BENCH_CONCURRENCY", 16);
        byte[] payload = new byte[size];
        random.nextBytes(payload);
        storage.write(new ObjectStorage.WriteOptions(), benchPrefix + "cwarm", Unpooled.wrappedBuffer(payload))
            .get(60, TimeUnit.SECONDS);

        // Bound in-flight requests to `concurrency`; aggregate throughput reflects the gateway under parallel load.
        Semaphore permits = new Semaphore(concurrency);
        long[] latencies = new long[count];
        List<CompletableFuture<?>> futures = new ArrayList<>(count);
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            permits.acquire();
            final int idx = i;
            long submit = System.nanoTime();
            CompletableFuture<?> cf = storage.write(new ObjectStorage.WriteOptions(),
                benchPrefix + "c/" + i, Unpooled.wrappedBuffer(payload));
            cf.whenComplete((r, e) -> {
                latencies[idx] = System.nanoTime() - submit;
                permits.release();
            });
            futures.add(cf);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(600, TimeUnit.SECONDS);
        long wallNanos = System.nanoTime() - start;

        System.out.println("\n=== Raw WRITE throughput (concurrency=" + concurrency + ") ===");
        log("=== Raw WRITE throughput concurrency=" + concurrency + " size=" + humanSize(size) + " count=" + count + " ===");
        log(header());
        printStats("conc-write", latencies, (long) size * count, wallNanos);
    }

    @Test
    public void walAppendPacedSteadyState() throws Exception {
        int records = envInt("BENCH_WAL_PACED_RECORDS", 500);
        int recordSize = envInt("BENCH_WAL_RECORD_SIZE", 1024);
        int rate = envInt("BENCH_WAL_RATE", 100);
        long maxBytesInBatch = envInt("BENCH_WAL_BATCH_BYTES", 1024 * 1024);
        long batchIntervalMs = envInt("BENCH_WAL_BATCH_INTERVAL_MS", 10);

        ObjectWALConfig config = ObjectWALConfig.builder()
            .withClusterId(clusterId)
            .withNodeId(NODE_ID)
            .withEpoch(0L)
            .withBucketId(storage.bucketId())
            .withMaxBytesInBatch(maxBytesInBatch)
            .withBatchInterval(batchIntervalMs)
            .withReservationService(new ObjectReservationService(clusterId, storage, storage.bucketId()))
            .build();
        new ObjectReservationService(clusterId, storage, storage.bucketId()).acquire(NODE_ID, 0L, false).join();
        ObjectWALService wal = new ObjectWALService(time, storage, config);
        wal.start();

        List<CompletableFuture<AppendResult>> warmup = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            warmup.add(wal.append(TraceContext.DEFAULT, StreamRecordBatch.of(1L, 0, i, 1, payload(recordSize), DefaultByteBufSupplier.INSTANCE)));
        }
        CompletableFuture.allOf(warmup.toArray(new CompletableFuture[0])).get(120, TimeUnit.SECONDS);

        // Submit at a fixed target rate so latency reflects steady-state commit time, not burst-drain queueing.
        long intervalNanos = 1_000_000_000L / rate;
        long[] latencies = new long[records];
        List<CompletableFuture<AppendResult>> futures = new ArrayList<>(records);
        long start = System.nanoTime();
        for (int i = 0; i < records; i++) {
            long target = start + i * intervalNanos;
            long wait = target - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
            }
            final int idx = i;
            long submit = System.nanoTime();
            CompletableFuture<AppendResult> cf = wal.append(TraceContext.DEFAULT,
                StreamRecordBatch.of(1L, 0, 200L + i, 1, payload(recordSize), DefaultByteBufSupplier.INSTANCE));
            cf.whenComplete((r, e) -> latencies[idx] = System.nanoTime() - submit);
            futures.add(cf);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(300, TimeUnit.SECONDS);
        long wallNanos = System.nanoTime() - start;

        System.out.println("\n=== WAL append PACED steady-state ===");
        log("=== WAL append PACED rate=" + rate + "/s records=" + records + " recordSize=" + humanSize(recordSize) + " ===");
        log(header());
        printStats("wal-paced", latencies, (long) records * recordSize, wallNanos);

        wal.shutdownGracefully();
    }

    // ---- helpers ----

    private void printStats(String label, long[] nanos, long totalBytes) {
        long sum = Arrays.stream(nanos).sum();
        printStats(label, nanos, totalBytes, sum);
    }

    private void printStats(String label, long[] nanos, long totalBytes, long wallNanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        double avgMs = Arrays.stream(sorted).average().orElse(0) / 1e6;
        double throughputMBs = totalBytes / 1024.0 / 1024.0 / (wallNanos / 1e9);
        double opsPerSec = sorted.length / (wallNanos / 1e9);
        log(String.format("%-10s | %8.2f | %8.2f | %8.2f | %8.2f | %8.2f | %8.2f | %9.1f | %8.2f",
            label,
            avgMs,
            ms(percentile(sorted, 0.50)),
            ms(percentile(sorted, 0.90)),
            ms(percentile(sorted, 0.99)),
            ms(percentile(sorted, 0.999)),
            ms(sorted[sorted.length - 1]),
            opsPerSec,
            throughputMBs));
    }

    private void log(String line) {
        System.out.println(line);
        report.append(line).append('\n');
    }

    private static String header() {
        return String.format("%-10s | %8s | %8s | %8s | %8s | %8s | %8s | %9s | %8s",
            "label", "avg(ms)", "p50", "p90", "p99", "p999", "max", "ops/s", "MB/s");
    }

    private static long percentile(long[] sorted, double p) {
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }

    private static String humanSize(int bytes) {
        if (bytes >= 1024 * 1024) {
            return (bytes / 1024 / 1024) + "MB";
        }
        if (bytes >= 1024) {
            return (bytes / 1024) + "KB";
        }
        return bytes + "B";
    }

    private ByteBuf payload(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return Unpooled.wrappedBuffer(bytes);
    }

    private void deleteByPrefix(String prefix) throws Exception {
        List<ObjectStorage.ObjectPath> objects = new ArrayList<>(storage.list(prefix).get(120, TimeUnit.SECONDS));
        if (!objects.isEmpty()) {
            storage.delete(objects).get(120, TimeUnit.SECONDS);
        }
    }

    private static int envInt(String name, int defaultVal) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? defaultVal : Integer.parseInt(v.trim());
    }
}
