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

import kafka.automq.table.WorkloadIdentityRESTCatalog;
import kafka.automq.table.binder.RecordBinder;
import kafka.automq.table.process.DefaultRecordProcessor;
import kafka.automq.table.process.ProcessingResult;
import kafka.automq.table.process.convert.AvroRegistryConverter;
import kafka.automq.table.process.convert.StringConverter;
import kafka.automq.table.process.transform.FlattenTransform;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full-stack live smoke test tying together every piece of the table-topic Lakehouse path with a real registered
 * schema: it resolves an Avro schema by id from the real MT Schema Registry ({@code convert.value.type=by_schema_id}),
 * runs the production convert + flatten pipeline to a strongly-typed record, derives the Iceberg schema via
 * {@link RecordBinder}, creates a table in Unity Catalog through {@link WorkloadIdentityRESTCatalog}, writes the typed
 * rows as Parquet to real MT HDFS through {@link WebHdfsFileIO}, commits, and reads them back with typed columns.
 *
 * <p>Disabled unless {@code AAD_TOKEN} and {@code SR_TABLE_SMOKE=true} are set. Optional env: {@code SR_URL},
 * {@code SR_SUBCLUSTER}, {@code SR_SCHEMA_ID} (default 68), {@code UC_URI}, {@code UC_WAREHOUSE}, {@code UC_NAMESPACE},
 * {@code UC_SUBCLUSTER}, {@code UC_TABLE_BASE}.
 *
 * <pre>
 * $env:AAD_TOKEN      = (az account get-access-token --resource api://1c0b8c88-563f-4b97-abdf-207172a50d2c --query accessToken -o tsv)
 * $env:SR_TABLE_SMOKE = "true"
 * ./gradlew :core:test --tests kafka.automq.table.io.SchemaRegistryTableWriteIT
 * </pre>
 */
@Timeout(240)
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "SR_TABLE_SMOKE", matches = "(?i)true")
public class SchemaRegistryTableWriteIT {

    private static final String DEFAULT_SR_URL = "https://api.magnetar.binginternal.com/schema-registry";
    private static final String DEFAULT_UC_URI =
        "https://api.magnetar.binginternal.com/unity-catalog/api/2.1/unity-catalog/iceberg";
    private static final String DEFAULT_SUBCLUSTER = "AKS-NorthEurope-FLEET";
    private static final int DEFAULT_SCHEMA_ID = 68;
    private static final int ROWS = 20;

    private WorkloadIdentityRESTCatalog catalog;
    private TableIdentifier tableId;

    @BeforeEach
    public void setup() {
        String uri = envOr("UC_URI", DEFAULT_UC_URI);
        String warehouse = envOr("UC_WAREHOUSE", "test_catalog1");
        String subcluster = envOr("UC_SUBCLUSTER", DEFAULT_SUBCLUSTER);

        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, uri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        props.put(CatalogProperties.FILE_IO_IMPL, "kafka.automq.table.io.WebHdfsFileIO");
        props.put("header.subcluster", subcluster);
        props.put("rest.client.connection-timeout-ms", "30000");
        props.put("rest.client.socket-timeout-ms", "120000");

        catalog = new WorkloadIdentityRESTCatalog();
        catalog.initialize("uc", props);
    }

    @AfterEach
    public void cleanup() {
        if (catalog != null) {
            try {
                if (tableId != null && catalog.tableExists(tableId)) {
                    catalog.dropTable(tableId, true);
                }
            } catch (Throwable t) {
                System.err.println("[SR-TABLE-IT] best-effort dropTable failed: " + t);
            } finally {
                try {
                    catalog.close();
                } catch (Exception ignored) {
                    // best effort
                }
            }
        }
    }

    @Test
    public void schemaRegistryToTypedIcebergTable() throws Exception {
        String srUrl = envOr("SR_URL", DEFAULT_SR_URL);
        String subcluster = envOr("SR_SUBCLUSTER", DEFAULT_SUBCLUSTER);
        int schemaId = Integer.parseInt(envOr("SR_SCHEMA_ID", String.valueOf(DEFAULT_SCHEMA_ID)));
        String namespace = envOr("UC_NAMESPACE", "automq_test");
        String tableBase = stripTrailingSlash(envOr("UC_TABLE_BASE",
            "hdfs://namenode0-vipv4.MTPrime-PROD-DUBE01.DUBE01.ap.gbl/user/pelian/test/falcon/table"));
        String token = System.getenv("AAD_TOKEN");

        // Real SR client: bearer + subcluster on every request (the only authorized surface is /schemas/ids/{id}).
        RestService restService = new RestService(srUrl);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);
        headers.put("subcluster", subcluster);
        restService.setHttpHeaders(headers);
        Map<String, Object> srCfg = new HashMap<>();
        srCfg.put("auto.register.schemas", false);
        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(
            restService, 100, List.of(new AvroSchemaProvider()), srCfg, null);

        ParsedSchema parsed = srClient.getSchemaById(schemaId);
        assertEquals("AVRO", parsed.schemaType(), "expected Avro schema at id " + schemaId);
        org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) parsed.rawSchema();
        System.out.println("[SR-TABLE-IT] resolved schema id=" + schemaId + " name=" + avroSchema.getFullName());

        // Production convert (by_schema_id Avro) + flatten pipeline.
        AvroRegistryConverter valueConverter = new AvroRegistryConverter(srClient, srUrl, false);
        DefaultRecordProcessor processor = new DefaultRecordProcessor(
            "sr-table-it", new StringConverter(), valueConverter, List.of(new FlattenTransform()));

        // Process ROWS records into typed Avro GenericRecords.
        List<org.apache.avro.generic.GenericRecord> typed = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            byte[] value = frame(schemaId, buildSample(avroSchema, i));
            SimpleRecord kafkaRecord = new SimpleRecord(i, System.currentTimeMillis(),
                ("k" + i).getBytes(StandardCharsets.UTF_8), value);
            ProcessingResult result = processor.process(0, kafkaRecord);
            if (!result.isSuccess()) {
                throw new AssertionError("processing failed at row " + i + ": "
                    + (result.getError() == null ? "?" : result.getError().getMessage()));
            }
            typed.add(result.getFinalRecord());
        }

        // Derive the Iceberg schema from the flattened Avro record and create the UC table on HDFS.
        RecordBinder schemaBinder = new RecordBinder(typed.get(0));
        org.apache.iceberg.Schema icebergSchema = schemaBinder.getIcebergSchema();
        System.out.println("[SR-TABLE-IT] iceberg columns: "
            + icebergSchema.columns().stream().map(c -> c.name() + ":" + c.type()).toList());

        tableId = TableIdentifier.of(Namespace.of(namespace),
            "sr_alltypes_it_" + UUID.randomUUID().toString().replace('-', '_'));
        String location = tableBase + "/" + tableId.name();
        Table table = catalog.buildTable(tableId, icebergSchema).withLocation(location).create();
        assertNotNull(table);

        // Bind against the table's (id-assigned) schema, then write typed rows as Parquet through WebHdfsFileIO.
        RecordBinder tableBinder = schemaBinder.createBinderForNewSchema(table.schema(), typed.get(0).getSchema());
        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
        DataWriter<Record> writer = appenderFactory.newDataWriter(
            fileFactory.newOutputFile(), FileFormat.PARQUET, null);
        try {
            for (org.apache.avro.generic.GenericRecord r : typed) {
                writer.write(tableBinder.bind(r));
            }
        } finally {
            writer.close();
        }
        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();

        // Reload from UC and read back; assert typed columns survived the whole path.
        Table reloaded = catalog.loadTable(tableId);
        assertNotNull(reloaded.currentSnapshot(), "table should have a snapshot after commit");
        assertNotNull(reloaded.schema().findField("id"), "typed column 'id' missing");
        assertNotNull(reloaded.schema().findField("name"), "typed column 'name' missing");
        assertNotNull(reloaded.schema().findField("address"), "nested column 'address' missing");
        assertEquals(org.apache.iceberg.types.Type.TypeID.LONG, reloaded.schema().findField("id").type().typeId());
        assertEquals(org.apache.iceberg.types.Type.TypeID.STRING, reloaded.schema().findField("name").type().typeId());

        List<Record> actual = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(reloaded).build()) {
            for (Record r : rows) {
                actual.add(r);
            }
        }
        assertEquals(ROWS, actual.size(), "row count mismatch");
        // Spot-check a value round-trip.
        boolean sawAlice0 = actual.stream().anyMatch(r ->
            "alice-0".equals(String.valueOf(r.getField("name"))) && Long.valueOf(0L).equals(r.getField("id")));
        assertTrue(sawAlice0, "expected row id=0 name=alice-0 to survive the round-trip");

        System.out.println("[SR-TABLE-IT] PASS: SR Avro schema -> typed Iceberg table on HDFS via UC, " + ROWS + " rows read back");
    }

    private static org.apache.avro.generic.GenericRecord buildSample(org.apache.avro.Schema schema, int i) {
        org.apache.avro.Schema addressSchema = unwrap(schema.getField("address").schema());
        org.apache.avro.generic.GenericRecord address = new GenericRecordBuilder(addressSchema)
            .set("city", "city-" + i)
            .set("zip", String.format("%05d", 10000 + i))
            .build();

        GenericData.Array<Object> tags = new GenericData.Array<>(2, unwrap(schema.getField("tags").schema()));
        tags.add("t" + i);
        tags.add("common");

        Map<String, String> props = new LinkedHashMap<>();
        props.put("idx", String.valueOf(i));
        props.put("kind", "sample");

        return new GenericRecordBuilder(schema)
            .set("id", (long) i)
            .set("name", "alice-" + i)
            .set("active", i % 2 == 0)
            .set("score", 1.5d * i)
            .set("ratio", 0.25f * i)
            .set("count", i)
            .set("payload", ByteBuffer.wrap(new byte[] {(byte) i, 2, 3}))
            .set("nickname", i % 3 == 0 ? null : "ali-" + i)
            .set("created_at", System.currentTimeMillis())
            .set("tags", tags)
            .set("props", props)
            .set("address", address)
            .build();
    }

    /** Confluent Avro wire format: magic byte 0x0 + 4-byte big-endian schema id + Avro binary payload. */
    private static byte[] frame(int schemaId, org.apache.avro.generic.GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x0);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<org.apache.avro.generic.GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static org.apache.avro.Schema unwrap(org.apache.avro.Schema s) {
        if (s.getType() != org.apache.avro.Schema.Type.UNION) {
            return s;
        }
        for (org.apache.avro.Schema branch : s.getTypes()) {
            if (branch.getType() != org.apache.avro.Schema.Type.NULL) {
                return branch;
            }
        }
        return s;
    }

    private static String envOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static String stripTrailingSlash(String s) {
        String v = s;
        while (v.endsWith("/")) {
            v = v.substring(0, v.length() - 1);
        }
        return v;
    }

    /** Minimal Kafka Record backed by fixed key/value bytes. */
    private static final class SimpleRecord implements org.apache.kafka.common.record.Record {
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
