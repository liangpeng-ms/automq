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

package kafka.log.stream.s3.wal;

import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.IdURI;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that {@link DefaultWalFactory} routes the {@code hdfs} WAL protocol to an object-storage WAL backed by the
 * HDFS object storage, and rejects unknown protocols. Construction is offline (no gateway contact), so no endpoint
 * needs to be reachable.
 */
@Tag("S3Unit")
public class DefaultWalFactoryTest {

    private final DefaultWalFactory factory = new DefaultWalFactory(1, Map.of(), null, null);

    @Test
    public void buildHdfsProtocolReturnsObjectWal() {
        IdURI uri = IdURI.parse("-3@hdfs://automq?endpoint=http://localhost:1/webhdfs/v1/x&token=dummy");
        WriteAheadLog wal = factory.build(uri, WalFactory.BuildOptions.builder().nodeEpoch(1L).build());
        assertInstanceOf(ObjectWALService.class, wal);
    }

    @Test
    public void buildUnsupportedProtocolThrows() {
        IdURI uri = IdURI.parse("-9@unknown://automq");
        assertThrows(IllegalArgumentException.class,
            () -> factory.build(uri, WalFactory.BuildOptions.builder().nodeEpoch(1L).build()));
    }
}
