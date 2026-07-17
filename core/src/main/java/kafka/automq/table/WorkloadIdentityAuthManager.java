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

package kafka.automq.table;

import com.automq.stream.s3.operator.WorkloadIdentityTokenUtil;

import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

import java.util.Map;
import java.util.function.Supplier;

/**
 * An Iceberg {@link AuthManager} that authenticates every REST request with a fresh Entra ID (AAD) bearer token, so a
 * long-lived catalog never fails once an initially-configured static token expires. Selected via
 * {@code rest.auth.type=kafka.automq.table.WorkloadIdentityAuthManager}. Replaces the pre-1.10
 * RESTClient-decorator hack (Iceberg 1.6.1 had no AuthManager SPI).
 */
public class WorkloadIdentityAuthManager implements AuthManager {

    /** REST catalog config key holding a static bearer token (dev/tests). */
    public static final String TOKEN_PROP = "token";
    /** REST catalog config key holding the Azure Workload Identity token scope. */
    public static final String TOKEN_SCOPE_PROP = "token.scope";

    // Required by AuthManagers reflective loading (impl(name)).
    @SuppressWarnings("UnusedVariable")
    public WorkloadIdentityAuthManager(String name) {
    }

    /** Whether a bearer-token supplier can be resolved from the given catalog config / environment. */
    public static boolean canSupplyToken(Map<String, String> config) {
        return WorkloadIdentityTokenUtil.canSupply(config.get(TOKEN_PROP));
    }

    @Override
    public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
        Supplier<String> tokenSupplier = WorkloadIdentityTokenUtil.tokenSupplier(
            properties.get(TOKEN_PROP), properties.get(TOKEN_SCOPE_PROP), "REST catalog token");
        return new WorkloadIdentityAuthSession(tokenSupplier);
    }

    @Override
    public void close() {
        // no resources to release
    }

    private static final class WorkloadIdentityAuthSession implements AuthSession {
        private final Supplier<String> tokenSupplier;

        WorkloadIdentityAuthSession(Supplier<String> tokenSupplier) {
            this.tokenSupplier = tokenSupplier;
        }

        @Override
        public HTTPRequest authenticate(HTTPRequest request) {
            HTTPHeaders newHeaders = request.headers().putIfAbsent(
                HTTPHeaders.of(Map.of("Authorization", "Bearer " + tokenSupplier.get())));
            if (newHeaders.equals(request.headers())) {
                return request;
            }
            return ImmutableHTTPRequest.builder().from(request).headers(newHeaders).build();
        }

        @Override
        public void close() {
        }
    }
}
