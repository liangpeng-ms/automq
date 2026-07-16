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

import kafka.automq.table.deserializer.proto.LatestSchemaResolutionResolver;
import kafka.automq.table.deserializer.proto.ProtobufSchemaProvider;
import kafka.automq.table.process.ConversionResult;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.SchemaFormat;
import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;
import kafka.automq.table.process.exception.ProcessorInitializationException;
import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.server.record.TableTopicConvertType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class ConverterFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConverterFactory.class);

    private static final String VALUE_SUFFIX = "-value";
    private static final String PROTOBUF_TYPE = "PROTOBUF";
    // Confluent wire framing: magic byte (0x0) + 4-byte big-endian schema id.
    private static final byte MAGIC_BYTE = 0x0;
    private static final int SCHEMA_ID_HEADER_SIZE = 5;
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofMinutes(20);
    private static final int MAX_CACHE_SIZE = 10000;

    private final String schemaRegistryUrl;
    private final SchemaRegistryClient client;
    private final Map<String, Converter> converterCache = new ConcurrentHashMap<>();
    private final Cache<String, String> topicSchemaFormatCache = CacheBuilder.newBuilder()
        .expireAfterAccess(CACHE_EXPIRE_DURATION)
        .maximumSize(MAX_CACHE_SIZE)
        .build();

    public ConverterFactory(String registryUrl) {
        this(registryUrl, Map.of());
    }

    public ConverterFactory(String registryUrl, Map<String, ?> schemaRegistryClientConfigs) {
        this.schemaRegistryUrl = registryUrl;
        if (registryUrl != null && !registryUrl.trim().isEmpty()) {
            Map<String, ?> configs = Objects.requireNonNullElse(schemaRegistryClientConfigs, Map.of());
            RestService restService = new RestService(registryUrl);
            // Custom static headers (e.g. a gateway 'subcluster' routing header) must be injected via the httpHeaders
            // constructor parameter: RestService.configure(...) ignores request.header.* and only sends headers set
            // through setHttpHeaders(...). Bearer auth (including Workload Identity) is resolved by the Confluent client
            // through the BearerAuthCredentialProvider SPI (schema-registry-tool jar) via bearer.auth.credentials.source.
            this.client = new CachedSchemaRegistryClient(
                restService,
                AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
                List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider()),
                configs,
                extractHttpHeaders(configs)
            );
        } else {
            this.client = null;
        }
    }

    /**
     * Extracts static HTTP headers from {@code request.header.*} client configs so they are sent on every Schema
     * Registry request via the {@link CachedSchemaRegistryClient} {@code httpHeaders} constructor parameter. The
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
        String namespace = "schema.registry.";
        String headerPrefix = "request.header.";
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (key == null || entry.getValue() == null) {
                continue;
            }
            String stripped = key.startsWith(namespace) ? key.substring(namespace.length()) : key;
            if (stripped.startsWith(headerPrefix) && stripped.length() > headerPrefix.length()) {
                headers.put(stripped.substring(headerPrefix.length()), String.valueOf(entry.getValue()));
            }
        }
        return headers.isEmpty() ? null : headers;
    }

    public ConverterFactory(String registryUrl, SchemaRegistryClient client) {
        this.schemaRegistryUrl = registryUrl;
        this.client = client;
    }

    public Converter createKeyConverter(String topic, WorkerConfig config) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("WorkerConfig cannot be null");
        }
        TableTopicConvertType convertType = config.keyConvertType();
        String subject = config.keySubject();
        String messageName = config.keyMessageFullName();
        return createConverterByType(topic, convertType, subject, messageName, true);
    }

    public Converter createValueConverter(String topic, WorkerConfig config) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("WorkerConfig cannot be null");
        }
        TableTopicConvertType convertType = config.valueConvertType();
        String subject = config.valueSubject();
        String messageName = config.valueMessageFullName();
        return createConverterByType(topic, convertType, subject, messageName, false);
    }

    private Converter createConverterByType(String topic, TableTopicConvertType convertType, String subjectName, String messageName, boolean isKey) {
        switch (convertType) {
            case RAW:
                return new RawConverter();
            case STRING:
                return new StringConverter();
            case BY_SCHEMA_ID:
                return createForSchemaId(topic, isKey);
            case BY_LATEST_SCHEMA:
                return createForSubjectName(topic, subjectName, messageName, isKey);
            default:
                throw new IllegalArgumentException("Unsupported convert type: " + convertType);
        }
    }

    public Converter createForSchemaId(String topic, boolean isKey) {
        if (client == null) {
            throw new ProcessorInitializationException("Schema Registry client is not initialized");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }

        // Resolve the concrete format (Avro/Protobuf) from the schema id embedded in each record (magic byte + id)
        // via /schemas/ids/{id}, instead of a subject-name lookup. Some Schema Registries only authorize by-id reads,
        // so a subject lookup (getLatestSchemaMetadata) would be rejected even though by_schema_id conversion works.
        return new SchemaIdConverter(isKey);
    }

    public Converter createForSubjectName(String topic, String subjectName, String messageFullName, boolean isKey) {
        String subject = subjectName != null ? subjectName : getSubjectName(topic);
        return new LazyConverter(() -> {
            String schemaType = getSchemaType(subject);
            if (!PROTOBUF_TYPE.equals(schemaType)) {
                throw new ProcessorInitializationException(
                    String.format("by_subject_name is only supported for PROTOBUF, but got %s for subject %s", schemaType, subject));
            }

            String cacheKey = schemaType + "-" + subject + "-" + (messageFullName == null ? "" : messageFullName);
            return converterCache.computeIfAbsent(cacheKey, key -> {
                var resolver = new LatestSchemaResolutionResolver(client, subject, messageFullName);
                return new ProtobufRegistryConverter(client, schemaRegistryUrl, resolver, isKey);
            });
        });
    }

    private String getSchemaType(String subject) {
        if (client == null) {
            throw new ProcessorInitializationException("Schema Registry client is not available");
        }

        String schemaType = topicSchemaFormatCache.getIfPresent(subject);
        if (schemaType == null) {
            try {
                var metadata = client.getLatestSchemaMetadata(subject);
                if (metadata == null) {
                    throw new ProcessorInitializationException("No schema found for subject: " + subject);
                }
                schemaType = metadata.getSchemaType();
                if (schemaType != null) {
                    topicSchemaFormatCache.put(subject, schemaType);
                }
            } catch (IOException e) {
                LOGGER.error("IO error while fetching schema metadata for subject '{}'", subject, e);
                throw new ProcessorInitializationException("Failed to fetch schema metadata for subject: " + subject, e);
            } catch (RestClientException e) {
                LOGGER.error("Schema Registry error for subject '{}'", subject, e);
                throw new ProcessorInitializationException("Schema Registry error for subject: " + subject, e);
            }
        }
        return schemaType;
    }

    private String getSubjectName(String topic) {
        return topic + VALUE_SUFFIX;
    }

    private Converter createConverterForFormat(String format, boolean isKey) {
        LOGGER.info("Creating new converter for format: {}", format);
        SchemaFormat schemaFormat = SchemaFormat.fromString(format);
        switch (schemaFormat) {
            case AVRO:
                return new AvroRegistryConverter(client, schemaRegistryUrl, isKey);
            case PROTOBUF:
                return new ProtobufRegistryConverter(client, schemaRegistryUrl, isKey);
            default:
                LOGGER.error("Unsupported schema format '{}'", format);
                throw new ProcessorInitializationException("Unsupported schema format: " + format);
        }
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
     */
    private final class SchemaIdConverter implements Converter {
        private final boolean isKey;
        private volatile Converter delegate;

        SchemaIdConverter(boolean isKey) {
            this.isKey = isKey;
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
                    throw new ConverterException("Failed to resolve schema id " + schemaId + " from registry", e);
                }
                SchemaFormat format = SchemaFormat.fromString(schemaType);
                delegate = converterCache.computeIfAbsent(format.name(), f -> createConverterForFormat(f, isKey));
            }
            return delegate;
        }
    }

}
