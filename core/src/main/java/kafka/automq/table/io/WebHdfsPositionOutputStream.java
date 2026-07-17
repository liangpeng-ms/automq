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

import com.automq.stream.s3.webhdfs.WebHdfsClient;

import org.apache.iceberg.io.PositionOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link PositionOutputStream} over WebHDFS. Bytes are staged to a local temp file as they are written, then published
 * with a single one-shot {@code op=CREATE&data=true} on {@link #close()} (the parent directory is created first). This
 * gives S3-like all-or-nothing visibility: a crash mid-write only leaves a local temp file, never a partial HDFS object.
 */
class WebHdfsPositionOutputStream extends PositionOutputStream {
    private final WebHdfsClient client;
    private final String location;
    private final boolean overwrite;
    private final Path tmp;
    private final OutputStream out;
    private long pos;
    private boolean closed;

    WebHdfsPositionOutputStream(WebHdfsClient client, String location, boolean overwrite) throws IOException {
        this.client = client;
        this.location = location;
        this.overwrite = overwrite;
        this.tmp = Files.createTempFile("webhdfs-out-", ".tmp");
        this.out = new BufferedOutputStream(Files.newOutputStream(tmp));
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        pos++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        pos += len;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            out.close();
            client.create(location, tmp, overwrite);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
