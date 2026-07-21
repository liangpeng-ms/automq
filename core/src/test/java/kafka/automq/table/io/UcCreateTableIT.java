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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One-shot utility (run manually, not in CI) that pre-creates a Unity Catalog Iceberg table WITH an explicit
 * {@code hdfs://} location, so AutoMQ's Table Topic — which calls {@code catalog.createTable(id, schema, spec, props)}
 * without a location and would otherwise trip UC's "url is null" requirement — can simply {@code loadTable} it and
 * append/commit. AutoMQ evolves the schema (additive) at runtime, so a minimal {@code _kafka_value} schema suffices.
 *
 * <p>Run with (PowerShell), after refreshing the token:
 * <pre>
 * $env:AAD_TOKEN = (az account get-access-token --resource api://1c0b8c88-563f-4b97-abdf-207172a50d2c --query accessToken -o tsv)
 * $env:UC_CREATE_TABLE = "true"
 * # optional overrides: UC_NAMESPACE, UC_TABLE, UC_TABLE_LOCATION, UC_URI, UC_WAREHOUSE, UC_SUBCLUSTER
 * ./gradlew :core:test --tests kafka.automq.table.io.UcCreateTableIT
 * </pre>
 */
@Timeout(180)
@EnabledIfEnvironmentVariable(named = "AAD_TOKEN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "UC_CREATE_TABLE", matches = "(?i)true")
public class UcCreateTableIT {
    private static final String DEFAULT_URI =
        "https://api.magnetar.binginternal.com/unity-catalog/api/2.1/unity-catalog/iceberg";

    @Test
    public void createTableWithLocation() {
        String uri = envOr("UC_URI", DEFAULT_URI);
        String warehouse = envOr("UC_WAREHOUSE", "test_catalog1");
        String namespace = envOr("UC_NAMESPACE", "automq_test");
        String table = envOr("UC_TABLE", "test2");
        String subcluster = envOr("UC_SUBCLUSTER", "AKS-NorthEurope-FLEET");
        String location = stripTrailingSlash(envOr("UC_TABLE_LOCATION",
            "hdfs://namenode0-vipv4.MTPrime-PROD-DUBE01.DUBE01.ap.gbl/user/pelian/test/falcon/table/" + table));

        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, uri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        props.put(CatalogProperties.FILE_IO_IMPL, "com.automq.hdfs.table.io.WebHdfsFileIO");
        props.put("header.subcluster", subcluster);
        props.put("rest.client.connection-timeout-ms", "30000");
        props.put("rest.client.socket-timeout-ms", "120000");
        props.put("rest.auth.type", "com.automq.hdfs.table.auth.WorkloadIdentityAuthManager");

        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("uc", props);
        try {
            TableIdentifier id = TableIdentifier.of(Namespace.of(namespace), table);
            if (catalog.tableExists(id)) {
                System.out.println("[UcCreateTableIT] table already exists: " + id + " -> " + catalog.loadTable(id).location());
                return;
            }
            if (catalog instanceof SupportsNamespaces) {
                try {
                    ((SupportsNamespaces) catalog).loadNamespaceMetadata(Namespace.of(namespace));
                } catch (NoSuchNamespaceException e) {
                    try {
                        ((SupportsNamespaces) catalog).createNamespace(Namespace.of(namespace));
                    } catch (AlreadyExistsException ignored) {
                        // race; fine
                    }
                }
            }
            // Minimal, evolvable schema; AutoMQ adds _kafka_key/_kafka_header/_kafka_metadata as optional columns.
            Schema schema = new Schema(
                Types.NestedField.optional(1, "_kafka_value", Types.StringType.get()));
            org.apache.iceberg.Table created = catalog.buildTable(id, schema).withLocation(location).create();
            assertNotNull(created);
            assertTrue(created.location().startsWith("hdfs://"), "unexpected location: " + created.location());
            System.out.println("[UcCreateTableIT] created " + id + " at " + created.location());
        } finally {
            try {
                catalog.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
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
