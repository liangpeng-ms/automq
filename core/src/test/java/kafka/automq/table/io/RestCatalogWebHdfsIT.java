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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live combined smoke test: a real Iceberg {@link RESTCatalog} talking to the Unity Catalog Iceberg REST API, with
 * table data written through {@link WebHdfsFileIO} to the real MT HDFS (WebHDFS) — end to end. It creates a table,
 * writes a Parquet data file, commits it, reads the rows back, then drops the table.
 *
 * <p>Disabled unless {@code AAD_TOKEN} and {@code UC_SMOKE=true} are set (the token is short-lived, so refresh it right
 * before running). Run with (PowerShell):
 * <pre>
 * $env:AAD_TOKEN = (az account get-access-token --resource api://1c0b8c88-563f-4b97-abdf-207172a50d2c --query accessToken -o tsv)
 * $env:UC_SMOKE = "true"
 * ./gradlew :core:test --tests kafka.automq.table.io.RestCatalogWebHdfsIT
 * </pre>
 * Optional overrides: {@code UC_URI}, {@code UC_WAREHOUSE}, {@code UC_NAMESPACE}, {@code UC_SUBCLUSTER},
 * {@code UC_TABLE_BASE} (the hdfs:// root under which the test table is created).
 */
@Timeout(180)
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "UC_SMOKE", matches = "(?i)true")
public class RestCatalogWebHdfsIT {
    private static final String DEFAULT_URI =
        "https://api.magnetar.binginternal.com/unity-catalog/api/2.1/unity-catalog/iceberg";

    private RESTCatalog catalog;
    private TableIdentifier tableId;
    private String tableBase;

    @BeforeEach
    public void setup() {
        String uri = envOr("UC_URI", DEFAULT_URI);
        String warehouse = envOr("UC_WAREHOUSE", "test_catalog1");
        String namespace = envOr("UC_NAMESPACE", "automq_test");
        String subcluster = envOr("UC_SUBCLUSTER", "AKS-NorthEurope-FLEET");
        tableBase = stripTrailingSlash(envOr("UC_TABLE_BASE",
            "hdfs://namenode0-vipv4.MTPrime-PROD-DUBE01.DUBE01.ap.gbl/user/pelian/test/falcon/table"));

        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, uri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        props.put(CatalogProperties.FILE_IO_IMPL, "kafka.automq.table.io.WebHdfsFileIO");
        // UC requires the subcluster on every request; Iceberg sends header.* on all requests.
        props.put("header.subcluster", subcluster);
        // No static "token": the WorkloadIdentityAuthManager (rest.auth.type) injects a fresh bearer per request (WI
        // when available, else the AAD_TOKEN env). This validates the exact auth path that ships for in-cluster
        // Workload Identity.
        // UC's DELETE/commit can be slow (purge deletes HDFS files); give the REST client generous timeouts.
        props.put("rest.client.connection-timeout-ms", "30000");
        props.put("rest.client.socket-timeout-ms", "120000");
        props.put("rest.auth.type", "kafka.automq.table.WorkloadIdentityAuthManager");

        catalog = new RESTCatalog();
        catalog.initialize("uc", props);
        tableId = TableIdentifier.of(Namespace.of(namespace), "automq_webhdfs_it_" + UUID.randomUUID().toString().replace('-', '_'));
    }

    @AfterEach
    public void cleanup() {
        if (catalog != null) {
            try {
                if (tableId != null && catalog.tableExists(tableId)) {
                    catalog.dropTable(tableId, true);
                }
            } catch (Throwable t) {
                // Best-effort cleanup: a slow/failed drop (e.g. purge timeout) must not fail the smoke test, whose
                // meaningful assertions (create/write/commit/read) have already run in the test body.
                System.err.println("[RestCatalogWebHdfsIT] best-effort dropTable failed: " + t);
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
    public void createWriteCommitReadDrop() throws Exception {
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        // 1) create table in UC. UC requires an explicit storage location (url); Iceberg's createTable(id, schema)
        // sends none, so build the table with a location under the configured hdfs:// base.
        String location = tableBase + "/" + tableId.name();
        Table table = catalog.buildTable(tableId, schema).withLocation(location).create();
        assertNotNull(table);
        String actualLocation = table.location();
        assertTrue(actualLocation.startsWith("hdfs://") || actualLocation.startsWith("webhdfs://") || actualLocation.startsWith("swebhdfs://"),
            "unexpected table location scheme: " + actualLocation);

        // 2) build records
        List<Record> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            GenericRecord r = GenericRecord.create(schema);
            r.setField("id", (long) i);
            r.setField("name", "row-" + i);
            expected.add(r);
        }

        // 3) write a Parquet data file through the table's FileIO (WebHdfsFileIO) and commit it
        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1)
            .format(FileFormat.PARQUET)
            .build();
        DataFile dataFile;
        DataWriter<Record> writer = appenderFactory.newDataWriter(
            fileFactory.newOutputFile(), FileFormat.PARQUET, null);
        try {
            for (Record r : expected) {
                writer.write(r);
            }
        } finally {
            writer.close();
        }
        dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();

        // 4) reload and read the rows back
        Table reloaded = catalog.loadTable(tableId);
        assertNotNull(reloaded.currentSnapshot(), "table should have a snapshot after commit");
        List<Record> actual = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(reloaded).build()) {
            for (Record r : rows) {
                actual.add(r);
            }
        }
        assertEquals(expected.size(), actual.size(), "row count mismatch");
    }

    private static String envOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }

    private static String stripTrailingSlash(String s) {
        String v = s;
        while (v.endsWith("/")) {
            v = v.substring(0, v.length() - 1);
        }
        return v;
    }
}
