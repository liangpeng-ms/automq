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

package kafka.automq.table.io;

import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Throughput/latency micro-benchmark for the self-contained {@link WebHdfsFileIO} write path against a real MT WebHDFS
 * gateway. It isolates the raw single-gateway write ceiling (no Kafka, no WAL, no Iceberg metadata) by writing many
 * fixed-size files through the exact {@code newOutputFile -> createOrOverwrite -> write} path that AutoMQ table topics
 * use for Iceberg data files, sweeping write concurrency to reveal where throughput plateaus (the single-gateway
 * ceiling) and where added parallelism stops helping.
 *
 * <p>Disabled unless {@code AAD_TOKEN}, {@code HDFS_TABLE_BASE} and {@code HDFS_PERF=true} are all set, so it never runs
 * in the normal CI selection.
 *
 * <p>Run with (PowerShell):
 * <pre>
 * $env:AAD_TOKEN     = (az account get-access-token --resource api://1c0b8c88-563f-4b97-abdf-207172a50d2c --query accessToken -o tsv)
 * $env:HDFS_TABLE_BASE = "hdfs://namenode0-vipv4.MTPrime-PROD-DUBE01.DUBE01.ap.gbl/user/pelian/test/falcon"
 * $env:HDFS_PERF     = "true"
 * # optional tuning:
 * $env:HDFS_PERF_FILE_MB     = "8"          # size of each written file (MiB)
 * $env:HDFS_PERF_FILES       = "32"         # files written per concurrency level
 * $env:HDFS_PERF_CONCURRENCY = "1,2,4,8,16,32"  # concurrency levels to sweep
 * ./gradlew :core:test --tests kafka.automq.table.io.WebHdfsPerfIT
 * </pre>
 */
@Timeout(1800)
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "HDFS_TABLE_BASE", matches = ".+")
@EnabledIfEnvironmentVariable(named = "HDFS_PERF", matches = "(?i)true")
public class WebHdfsPerfIT {
    private WebHdfsFileIO io;
    private String runDir;

    @BeforeEach
    public void setup() {
        io = new WebHdfsFileIO();
        // No static token/gateway: token falls back to AAD_TOKEN env; gateway derived from the hdfs:// authority.
        io.initialize(Map.of());
        String base = System.getenv("HDFS_TABLE_BASE");
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        runDir = base + "/webhdfs-perf-it/" + UUID.randomUUID();
    }

    @AfterEach
    public void cleanup() {
        if (io != null) {
            io.close();
        }
    }

    @Test
    public void writeThroughputCeilingSweep() throws Exception {
        int fileMb = envInt("HDFS_PERF_FILE_MB", 8);
        int files = envInt("HDFS_PERF_FILES", 32);
        int[] levels = envLevels("HDFS_PERF_CONCURRENCY", new int[] {1, 2, 4, 8, 16, 32});
        int fileBytes = fileMb * 1024 * 1024;

        // Reuse one payload buffer across all writes; content is irrelevant for a write-bandwidth test.
        byte[] payload = new byte[fileBytes];
        for (int i = 0; i < fileBytes; i++) {
            payload[i] = (byte) i;
        }

        System.out.println("=== WebHDFS write throughput sweep ===");
        System.out.printf("fileSize=%dMiB filesPerLevel=%d perLevelData=%dMiB base=%s%n",
            fileMb, files, (long) fileMb * files, runDir);
        System.out.println("concurrency |  wall(s) | throughput(MB/s) | perFile p50/p95/p99/max (ms)");

        List<String> allWritten = Collections.synchronizedList(new ArrayList<>());
        try {
            for (int c : levels) {
                Result r = runLevel(c, files, payload, allWritten);
                System.out.printf("%11d | %8.2f | %16.2f | %d / %d / %d / %d%n",
                    c, r.wallMs / 1000.0, r.mbPerSec, r.p50Ms, r.p95Ms, r.p99Ms, r.maxMs);
            }
        } finally {
            deleteAll(allWritten);
        }
    }

    private Result runLevel(int concurrency, int files, byte[] payload, List<String> written) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        ConcurrentLinkedQueue<Long> durationsNs = new ConcurrentLinkedQueue<>();
        AtomicInteger idx = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(files);
        String levelDir = runDir + "/c" + concurrency;

        long start = System.nanoTime();
        for (int i = 0; i < files; i++) {
            pool.submit(() -> {
                String loc = levelDir + "/part-" + idx.getAndIncrement() + ".bin";
                long t0 = System.nanoTime();
                try {
                    OutputFile out = io.newOutputFile(loc);
                    try (PositionOutputStream os = out.createOrOverwrite()) {
                        os.write(payload);
                    }
                    written.add(loc);
                    durationsNs.add(System.nanoTime() - t0);
                } catch (Exception e) {
                    System.out.println("write failed " + loc + ": " + e.getMessage());
                } finally {
                    done.countDown();
                }
            });
        }
        done.await();
        long wallNs = System.nanoTime() - start;
        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        long totalBytes = (long) durationsNs.size() * payload.length;
        double wallMs = wallNs / 1_000_000.0;
        double mbPerSec = totalBytes / (1024.0 * 1024.0) / (wallNs / 1_000_000_000.0);

        List<Long> ds = new ArrayList<>(durationsNs);
        Collections.sort(ds);
        Result r = new Result();
        r.wallMs = wallMs;
        r.mbPerSec = mbPerSec;
        r.p50Ms = pctMs(ds, 50);
        r.p95Ms = pctMs(ds, 95);
        r.p99Ms = pctMs(ds, 99);
        r.maxMs = ds.isEmpty() ? 0 : ds.get(ds.size() - 1) / 1_000_000;
        return r;
    }

    private void deleteAll(List<String> locations) {
        for (String loc : locations) {
            try {
                io.deleteFile(loc);
            } catch (Exception ignore) {
                // best-effort cleanup on the shared gateway
            }
        }
    }

    private static long pctMs(List<Long> sortedNs, int pct) {
        if (sortedNs.isEmpty()) {
            return 0;
        }
        int i = (int) Math.ceil(pct / 100.0 * sortedNs.size()) - 1;
        i = Math.max(0, Math.min(i, sortedNs.size() - 1));
        return sortedNs.get(i) / 1_000_000;
    }

    private static int envInt(String name, int def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : Integer.parseInt(v.trim());
    }

    private static int[] envLevels(String name, int[] def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            return def;
        }
        String[] parts = v.split(",");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Integer.parseInt(parts[i].trim());
        }
        return out;
    }

    private static final class Result {
        double wallMs;
        double mbPerSec;
        long p50Ms;
        long p95Ms;
        long p99Ms;
        long maxMs;
    }
}
