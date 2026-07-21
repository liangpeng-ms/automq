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

import kafka.automq.table.process.ConversionResult;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.SchemaFormat;
import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;

import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Confluent Schema Registry helpers used by {@link ConverterFactory}, grouped here so the factory stays close to its
 * upstream shape: HTTP-header extraction from client configs, Confluent wire-framing parsing, and the
 * {@code by_schema_id} converter.
 */
final class ConfluentSchemaRegistry {
    private static final String NAMESPACE = "schema.registry.";
    private static final String HEADER_PREFIX = "request.header.";
    // Confluent wire framing: magic byte (0x0) + 4-byte big-endian schema id.
    private static final byte MAGIC_BYTE = 0x0;
    private static final int SCHEMA_ID_HEADER_SIZE = 5;

    private ConfluentSchemaRegistry() {
    }

    /**
     * Extracts static HTTP headers from {@code request.header.*} client configs so they are sent on every Schema
     * Registry request via the {@code CachedSchemaRegistryClient} {@code httpHeaders} constructor parameter. The
     * Confluent {@code RestService.configure(...)} path does not read {@code request.header.*}; only headers set
     * through {@code setHttpHeaders(...)} are sent. Keys may carry the Confluent client namespace prefix
     * ({@code schema.registry.}) that AutoMQ prepends, so both prefixed and bare forms are handled.
     *
     * @return the header name/value map, or {@code null} when none are configured
     */
    static Map<String, String> extractHttpHeaders(Map<String, ?> configs) {
        if (configs == null || configs.isEmpty()) {
            return null;
        }
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (key == null || entry.getValue() == null) {
                continue;
            }
            String stripped = key.startsWith(NAMESPACE) ? key.substring(NAMESPACE.length()) : key;
            if (stripped.startsWith(HEADER_PREFIX) && stripped.length() > HEADER_PREFIX.length()) {
                headers.put(stripped.substring(HEADER_PREFIX.length()), String.valueOf(entry.getValue()));
            }
        }
        return headers.isEmpty() ? null : headers;
    }

    /** Reads the Confluent-framed schema id (magic byte + 4-byte big-endian id) without consuming the buffer. */
    static int readSchemaId(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < SCHEMA_ID_HEADER_SIZE) {
            throw new InvalidDataException("Invalid payload size: " + (buffer == null ? 0 : buffer.remaining())
                + ", expected at least " + SCHEMA_ID_HEADER_SIZE);
        }
        ByteBuffer buf = buffer.duplicate();
        byte magicByte = buf.get();
        if (magicByte != MAGIC_BYTE) {
            throw new InvalidDataException("Unknown magic byte: " + magicByte);
        }
        return buf.getInt();
    }

    /**
     * Converter for {@code by_schema_id} that determines the concrete format (Avro/Protobuf) from the schema id carried
     * by each record, resolved through {@code /schemas/ids/{id}} — the only Schema Registry surface some deployments
     * authorize. The concrete delegate is built lazily on the first record and cached (double-checked locking).
     *
     * <p>Unlike {@link LazyConverter}, whose no-arg supplier resolves the delegate from information known at
     * construction time (e.g. a subject name), this converter must inspect the first record's Confluent wire framing
     * to learn the schema id, so its resolution is driven by the buffer rather than a {@link java.util.function.Supplier}.</p>
     */
    static final class SchemaIdConverter implements Converter {
        private final SchemaRegistryClient client;
        private final Function<String, Converter> converterForFormat;
        private volatile Converter delegate;

        /**
         * @param client             the Schema Registry client used to resolve a schema id to its type
         * @param converterForFormat resolves a {@link SchemaFormat#name()} to a (cached) format-specific converter
         */
        SchemaIdConverter(SchemaRegistryClient client, Function<String, Converter> converterForFormat) {
            this.client = client;
            this.converterForFormat = converterForFormat;
        }

        @Override
        public ConversionResult convert(String topic, ByteBuffer buffer) throws ConverterException {
            Converter d = delegate;
            if (d == null) {
                d = initDelegate(buffer);
            }
            return d.convert(topic, buffer);
        }

        private synchronized Converter initDelegate(ByteBuffer buffer) throws ConverterException {
            if (delegate == null) {
                int schemaId = readSchemaId(buffer);
                String schemaType;
                try {
                    ParsedSchema parsedSchema = client.getSchemaById(schemaId);
                    schemaType = parsedSchema.schemaType();
                } catch (RestClientException | IOException e) {
                    // Mirror the Confluent deserializer: a registry lookup failure surfaces as a KafkaException
                    // (SerializationException) whose cause is the original RestClientException/IOException, so
                    // DefaultRecordProcessor classifies it exactly as the delegate deserializer would. Wrapping it in a
                    // ConverterException here would hide the cause the error handling and callers rely on.
                    throw new SerializationException("Failed to resolve schema id " + schemaId + " from registry", e);
                }
                SchemaFormat format = SchemaFormat.fromString(schemaType);
                delegate = converterForFormat.apply(format.name());
            }
            return delegate;
        }
    }
}
