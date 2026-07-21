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

package kafka.automq.table.process;

import kafka.automq.table.process.convert.AvroRegistryConverter;
import kafka.automq.table.process.convert.StringConverter;
import kafka.automq.table.process.transform.FlattenTransform;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration test validating the AutoMQ table-topic schema-registry path against the real MT Schema Registry
 * (Confluent-compatible) behind the api-gateway, using Azure AD bearer auth + the {@code subcluster} request header,
 * exactly like {@code convert.value.type=by_schema_id}.
 *
 * <p>It resolves a pre-registered Avro schema by id via {@code /schemas/ids/{id}} (the only SR surface authorized for
 * our identity), hand-frames a Confluent Avro record (magic byte + schema id + Avro binary), and runs the production
 * {@link AvroRegistryConverter} + {@link FlattenTransform} pipeline. It asserts the record deserializes and flattens
 * into a strongly-typed schema (one Iceberg column per Avro field) instead of the schemaless {@code _kafka_value}.
 *
 * <p>Disabled unless {@code AAD_TOKEN} and {@code SR_SMOKE=true} are set. Optional env: {@code SR_URL},
 * {@code SR_SUBCLUSTER}, {@code SR_SCHEMA_ID} (defaults: MT gateway, AKS-NorthEurope-FLEET, 68).
 *
 * <pre>
 * $env:AAD_TOKEN = (az account get-access-token --resource api://1c0b8c88-563f-4b97-abdf-207172a50d2c --query accessToken -o tsv)
 * $env:SR_SMOKE  = "true"
 * ./gradlew :core:test --tests kafka.automq.table.process.SchemaRegistryAvroIT
 * </pre>
 */
@Timeout(120)
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "SR_SMOKE", matches = "(?i)true")
public class SchemaRegistryAvroIT {

    private static final String DEFAULT_SR_URL = "https://api.magnetar.binginternal.com/schema-registry";
    private static final String DEFAULT_SUBCLUSTER = "AKS-NorthEurope-FLEET";
    private static final int DEFAULT_SCHEMA_ID = 68;
    private static final String TOPIC = "sr-alltypes-it";

    @Test
    void resolveBySchemaIdAndFlattenToTypedColumns() throws Exception {
        String srUrl = envOr("SR_URL", DEFAULT_SR_URL);
        String subcluster = envOr("SR_SUBCLUSTER", DEFAULT_SUBCLUSTER);
        int schemaId = Integer.parseInt(envOr("SR_SCHEMA_ID", String.valueOf(DEFAULT_SCHEMA_ID)));
        String token = System.getenv("AAD_TOKEN");

        // Send the AAD bearer token + subcluster header explicitly on every SR request (same headers that return 200
        // for /schemas/ids/{id} via raw HTTP). Setting them on the RestService is more reliable than relying on the
        // Confluent client's bearer-auth config plumbing behind the MT api-gateway.
        RestService restService = new RestService(srUrl);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);
        headers.put("subcluster", subcluster);
        restService.setHttpHeaders(headers);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("auto.register.schemas", false);
        SchemaRegistryClient client = new CachedSchemaRegistryClient(
            restService, 100, List.of(new AvroSchemaProvider()), cfg, null);

        // Resolve the pre-registered Avro schema by id (the only authorized SR surface for our identity).
        ParsedSchema parsed = client.getSchemaById(schemaId);
        assertEquals("AVRO", parsed.schemaType(), "expected an Avro schema at id " + schemaId);
        Schema avroSchema = (Schema) parsed.rawSchema();
        System.out.println("[SR-IT] resolved schema id=" + schemaId + " name=" + avroSchema.getFullName());

        // Build a sample record covering every field type and Confluent-frame it with the resolved schema id.
        byte[] value = frame(schemaId, buildSample(avroSchema));
        Record kafkaRecord = new SimpleRecord(1L, System.currentTimeMillis(), "k1".getBytes(StandardCharsets.UTF_8), value);

        // Production pipeline: by_schema_id Avro convert -> flatten -> typed record.
        AvroRegistryConverter valueConverter = new AvroRegistryConverter(client, srUrl, false);
        DefaultRecordProcessor processor = new DefaultRecordProcessor(
            TOPIC, new StringConverter(), valueConverter, List.of(new FlattenTransform()));

        ProcessingResult result = processor.process(0, kafkaRecord);
        if (!result.isSuccess()) {
            throw new AssertionError("processing failed: " + (result.getError() == null ? "?" : result.getError().getMessage()));
        }

        Schema finalSchema = result.getFinalSchema();
        GenericRecord finalRecord = result.getFinalRecord();
        System.out.println("[SR-IT] flattened columns: " + finalSchema.getFields().stream().map(Schema.Field::name).toList());

        // Every Avro field must land as its own typed column (not a single schemaless _kafka_value blob).
        assertFieldType(finalSchema, "id", Schema.Type.LONG);
        assertFieldType(finalSchema, "name", Schema.Type.STRING);
        assertFieldType(finalSchema, "active", Schema.Type.BOOLEAN);
        assertFieldType(finalSchema, "score", Schema.Type.DOUBLE);
        assertFieldType(finalSchema, "ratio", Schema.Type.FLOAT);
        assertFieldType(finalSchema, "count", Schema.Type.INT);
        assertFieldType(finalSchema, "payload", Schema.Type.BYTES);
        assertUnionContains(finalSchema, "nickname", Schema.Type.STRING);
        // created_at is a long with a timestamp-millis logical type.
        assertEquals(Schema.Type.LONG, unwrap(fieldSchema(finalSchema, "created_at")).getType());
        assertFieldType(finalSchema, "tags", Schema.Type.ARRAY);
        assertFieldType(finalSchema, "props", Schema.Type.MAP);
        assertFieldType(finalSchema, "address", Schema.Type.RECORD);

        // Values survive the round-trip.
        assertEquals(42L, ((Number) finalRecord.get("id")).longValue());
        assertEquals("alice", finalRecord.get("name").toString());
        assertEquals(true, finalRecord.get("active"));
        assertNotNull(finalRecord.get("address"));

        System.out.println("[SR-IT] PASS: schema-registry by_schema_id -> typed Iceberg columns validated against real MT SR");
    }

    private static GenericRecord buildSample(Schema schema) {
        Schema addressSchema = unwrap(schema.getField("address").schema());
        GenericRecord address = new GenericRecordBuilder(addressSchema)
            .set("city", "Seattle")
            .set("zip", "98101")
            .build();

        GenericData.Array<Object> tags = new GenericData.Array<>(2, unwrap(schema.getField("tags").schema()));
        tags.add("a");
        tags.add("b");

        Map<String, String> props = new LinkedHashMap<>();
        props.put("k1", "v1");
        props.put("k2", "v2");

        return new GenericRecordBuilder(schema)
            .set("id", 42L)
            .set("name", "alice")
            .set("active", true)
            .set("score", 9.5d)
            .set("ratio", 1.25f)
            .set("count", 7)
            .set("payload", ByteBuffer.wrap(new byte[] {1, 2, 3}))
            .set("nickname", "ali")
            .set("created_at", System.currentTimeMillis())
            .set("tags", tags)
            .set("props", props)
            .set("address", address)
            .build();
    }

    /** Confluent Avro wire format: magic byte 0x0 + 4-byte big-endian schema id + Avro binary payload. */
    private static byte[] frame(int schemaId, GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x0);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static Schema fieldSchema(Schema record, String name) {
        Schema.Field f = record.getField(name);
        assertNotNull(f, "missing column: " + name);
        return f.schema();
    }

    /** Strip a nullable union ["null", X] down to X (and single-branch unions). */
    private static Schema unwrap(Schema s) {
        if (s.getType() != Schema.Type.UNION) {
            return s;
        }
        for (Schema branch : s.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch;
            }
        }
        return s;
    }

    private static void assertFieldType(Schema record, String name, Schema.Type expected) {
        assertEquals(expected, unwrap(fieldSchema(record, name)).getType(), "column " + name + " should be " + expected);
    }

    private static void assertUnionContains(Schema record, String name, Schema.Type expected) {
        assertEquals(expected, unwrap(fieldSchema(record, name)).getType(), "nullable column " + name + " should carry " + expected);
    }

    private static String envOr(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v.trim();
    }

    /** Minimal Kafka Record backed by fixed key/value bytes. */
    private static final class SimpleRecord implements Record {
        private final long offset;
        private final long timestamp;
        private final byte[] key;
        private final byte[] value;

        SimpleRecord(long offset, long timestamp, byte[] key, byte[] value) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }

        @Override public long offset() {
            return offset;
        }

        @Override public int sequence() {
            return -1;
        }

        @Override public int sizeInBytes() {
            return (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
        }

        @Override public long timestamp() {
            return timestamp;
        }

        @Override public void ensureValid() {
        }

        @Override public int keySize() {
            return key == null ? -1 : key.length;
        }

        @Override public boolean hasKey() {
            return key != null;
        }

        @Override public ByteBuffer key() {
            return key == null ? null : ByteBuffer.wrap(key);
        }

        @Override public int valueSize() {
            return value == null ? -1 : value.length;
        }

        @Override public boolean hasValue() {
            return value != null;
        }

        @Override public ByteBuffer value() {
            return value == null ? null : ByteBuffer.wrap(value);
        }

        @Override public boolean hasMagic(byte b) {
            return false;
        }

        @Override public boolean isCompressed() {
            return false;
        }

        @Override public boolean hasTimestampType(TimestampType type) {
            return false;
        }

        @Override public Header[] headers() {
            return new Header[0];
        }
    }
}
