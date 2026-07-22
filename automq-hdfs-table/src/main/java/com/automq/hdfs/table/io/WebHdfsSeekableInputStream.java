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

import com.automq.hdfs.common.webhdfs.WebHdfsClient;

import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link SeekableInputStream} over WebHDFS. Reads are served by a lazily-opened ranged {@code op=OPEN} stream starting
 * at the current position; {@link #seek(long)} simply drops the current stream so the next read re-opens at the new
 * offset. This matches the access pattern of Parquet readers (read footer near EOF, then seek to each column chunk).
 */
class WebHdfsSeekableInputStream extends SeekableInputStream {
    private final WebHdfsClient client;
    private final String location;
    private long pos;
    private InputStream in;
    private boolean closed;

    WebHdfsSeekableInputStream(WebHdfsClient client, String location) {
        this.client = client;
        this.location = location;
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public void seek(long newPos) {
        if (newPos < 0) {
            throw new IllegalArgumentException("negative seek: " + newPos);
        }
        if (newPos != pos) {
            closeStream();
            pos = newPos;
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        int b = in.read();
        if (b >= 0) {
            pos++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        int n = in.read(b, off, len);
        if (n > 0) {
            pos += n;
        }
        return n;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("stream closed");
        }
        if (in == null) {
            in = client.open(location, pos);
        }
    }

    private void closeStream() {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {
                // best effort
            }
            in = null;
        }
    }

    @Override
    public void close() {
        closed = true;
        closeStream();
    }
}
