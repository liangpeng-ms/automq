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

package kafka.automq.table.process.convert;

import kafka.automq.table.process.exception.InvalidDataException;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConverterFactoryTest {

    @Test
    void extractHttpHeadersReturnsNullWhenNoHeaderConfigs() {
        assertNull(ConverterFactory.extractHttpHeaders(null));
        assertNull(ConverterFactory.extractHttpHeaders(Map.of()));
        assertNull(ConverterFactory.extractHttpHeaders(Map.of(
            "schema.registry.bearer.auth.credentials.source", "FEDERATED_MANAGED_IDENTITY",
            "schema.registry.auto.register.schemas", "false")));
    }

    @Test
    void extractHttpHeadersHandlesNamespacedAndBareKeys() {
        Map<String, Object> configs = new HashMap<>();
        // AutoMQ prepends the "schema.registry." client namespace to every config key.
        configs.put("schema.registry.request.header.subcluster", "AKS-NorthEurope-FLEET");
        // A bare key (no namespace) must also be picked up.
        configs.put("request.header.x-trace", "abc123");
        // Non-header configs must be ignored (they flow through configure(...) instead).
        configs.put("schema.registry.bearer.auth.credentials.source", "FEDERATED_MANAGED_IDENTITY");

        Map<String, String> headers = ConverterFactory.extractHttpHeaders(configs);

        assertEquals(2, headers.size());
        assertEquals("AKS-NorthEurope-FLEET", headers.get("subcluster"));
        assertEquals("abc123", headers.get("x-trace"));
    }

    @Test
    void extractHttpHeadersSkipsNullValuesAndEmptyHeaderName() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("schema.registry.request.header.subcluster", null);
        configs.put("schema.registry.request.header.", "no-name");

        assertNull(ConverterFactory.extractHttpHeaders(configs));
    }

    @Test
    void workloadIdentityTokenSupplierNullWhenSourceIsNotWorkloadIdentity() {
        assertNull(ConverterFactory.workloadIdentityTokenSupplier(Map.of()));
        assertNull(ConverterFactory.workloadIdentityTokenSupplier(Map.of(
            "bearer.auth.credentials.source", "STATIC_TOKEN")));
    }

    @Test
    void workloadIdentityTokenSupplierUsesStaticTokenTierAndNamespacedKeys() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("schema.registry.bearer.auth.credentials.source", "workload_identity");
        configs.put("schema.registry.bearer.auth.token", "static-abc");

        Supplier<String> supplier = ConverterFactory.workloadIdentityTokenSupplier(configs);

        assertNotNull(supplier);
        assertEquals("static-abc", supplier.get());
    }

    @Test
    void readSchemaIdParsesConfluentFramingWithoutConsumingBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put((byte) 0x0);      // magic byte
        buffer.putInt(68);           // schema id
        buffer.putInt(0x01020304);   // payload
        buffer.flip();

        assertEquals(68, ConverterFactory.readSchemaId(buffer));
        // The reader must not advance the caller's buffer position.
        assertEquals(0, buffer.position());
        assertEquals(9, buffer.remaining());
    }

    @Test
    void readSchemaIdRejectsBadMagicAndShortBuffer() {
        ByteBuffer badMagic = ByteBuffer.allocate(5);
        badMagic.put((byte) 0x1);
        badMagic.putInt(68);
        badMagic.flip();
        assertThrows(InvalidDataException.class, () -> ConverterFactory.readSchemaId(badMagic));

        ByteBuffer tooShort = ByteBuffer.allocate(3);
        tooShort.flip();
        assertThrows(InvalidDataException.class, () -> ConverterFactory.readSchemaId(tooShort));

        assertThrows(InvalidDataException.class, () -> ConverterFactory.readSchemaId(null));
    }
}
