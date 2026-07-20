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

import com.automq.hdfs.token.WorkloadIdentityTokenUtil;
import com.automq.stream.s3.webhdfs.WebHdfsClient;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Iceberg {@link FileIO} backed directly by the WebHDFS HTTP gateway (no Hadoop dependency). It lets AutoMQ Table Topic
 * write Parquet data/metadata files to HDFS through the same WebHDFS + Entra ID (AAD) bearer path already used by the
 * AutoMQ WAL/object-storage backend, by simply setting {@code io-impl=kafka.automq.table.io.WebHdfsFileIO}.
 *
 * <p>Locations use Hadoop's WebHDFS scheme convention: {@code webhdfs://host:port/path} (HTTP) or
 * {@code swebhdfs://host:port/path} (HTTPS).
 *
 * <p>Configuration (passed via catalog {@code io-impl} properties). The default token supplier resolves in the same
 * 3-tier order as the AutoMQ WAL backend:
 * <ol>
 *   <li>{@code webhdfs.token} – an explicit static bearer token (dev/tests) takes precedence;</li>
 *   <li>Azure Workload Identity – if configured (federated env present), an auto-refreshing Entra ID token is used,
 *       with scope from {@code webhdfs.token.scope} or env {@code HDFS_TOKEN_SCOPE};</li>
 *   <li>{@code AAD_TOKEN} env var – a non-refreshing fallback static token.</li>
 * </ol>
 * {@code webhdfs.gateway} (optional) overrides the WebHDFS HTTP gateway base (including {@code /webhdfs/v1[/mount]}).
 * By default the gateway is derived from each location's {@code hdfs://<namenode>/abs/path} authority (MT WebHDFS HTTP
 * v2 naming), and the location is rewritten to {@code gateway + absPath}.
 * {@code webhdfs.timeout.ms} sets the per-request timeout (default 60000). A pluggable {@link Supplier} token provider
 * can also be injected in-process via {@link #WebHdfsFileIO(Supplier)}.
 */
public class WebHdfsFileIO implements FileIO {
    private static final long serialVersionUID = 1L;

    public static final String TOKEN_PROP = "webhdfs.token";
    public static final String TOKEN_SCOPE_PROP = "webhdfs.token.scope";
    public static final String GATEWAY_PROP = "webhdfs.gateway";
    public static final String TIMEOUT_MS_PROP = "webhdfs.timeout.ms";

    private Map<String, String> properties = Collections.emptyMap();
    private transient Supplier<String> tokenSupplier;
    private transient volatile WebHdfsClient client;

    /** No-arg constructor required for reflective instantiation by Iceberg's CatalogUtil. */
    public WebHdfsFileIO() {
    }

    /** In-process constructor allowing an auto-refreshing token provider (e.g. Workload Identity). */
    public WebHdfsFileIO(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    @Override
    public void initialize(Map<String, String> props) {
        this.properties = props;
        if (this.tokenSupplier == null) {
            this.tokenSupplier = defaultTokenSupplier(props);
        }
        this.client = null;
    }

    /**
     * Resolve the default token supplier with the same 3-tier priority as the AutoMQ WAL backend:
     * static {@code webhdfs.token} &gt; Azure Workload Identity (auto-refresh) &gt; static {@code AAD_TOKEN} env.
     */
    private static Supplier<String> defaultTokenSupplier(Map<String, String> props) {
        return WorkloadIdentityTokenUtil.tokenSupplier(
            props.get(TOKEN_PROP), props.get(TOKEN_SCOPE_PROP), "WebHDFS token");
    }

    private WebHdfsClient client() {
        WebHdfsClient c = client;
        if (c == null) {
            synchronized (this) {
                c = client;
                if (c == null) {
                    long timeoutMs = Long.parseLong(properties.getOrDefault(TIMEOUT_MS_PROP, "60000"));
                    if (tokenSupplier == null) {
                        initialize(properties);
                    }
                    String gateway = properties.get(GATEWAY_PROP);
                    c = new WebHdfsClient(tokenSupplier, Duration.ofMillis(timeoutMs), gateway);
                    client = c;
                }
            }
        }
        return c;
    }

    @Override
    public InputFile newInputFile(String path) {
        return new WebHdfsInputFile(client(), path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return new WebHdfsInputFile(client(), path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return new WebHdfsOutputFile(client(), path);
    }

    @Override
    public void deleteFile(String path) {
        client().delete(path);
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public void close() {
        // JDK HttpClient has no explicit close; nothing to release.
    }
}
