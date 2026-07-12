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

import com.automq.stream.s3.operator.WorkloadIdentityTokenProvider;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ErrorResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A {@link RESTCatalog} that authenticates every request with a fresh Entra ID (AAD) bearer token, so long-lived
 * catalogs never fail once an initially-configured static token expires. The token is resolved (per request) in the
 * same 3-tier order used by the AutoMQ HDFS backend:
 * <ol>
 *   <li>static {@code token} property (dev/tests);</li>
 *   <li>Azure Workload Identity (auto-refreshing), scope from {@code token.scope} or env {@code HDFS_TOKEN_SCOPE};</li>
 *   <li>{@code AAD_TOKEN} env var (non-refreshing fallback).</li>
 * </ol>
 * Because Iceberg's stock {@link RESTCatalog} captures the token once, this subclass instead injects the
 * {@code Authorization} header on each request through a thin {@link RESTClient} decorator.
 */
public class WorkloadIdentityRESTCatalog extends RESTCatalog {
    public static final String TOKEN_PROP = "token";
    public static final String TOKEN_SCOPE_PROP = "token.scope";
    private static final String TOKEN_ENV = "AAD_TOKEN";
    private static final String TOKEN_SCOPE_ENV = "HDFS_TOKEN_SCOPE";

    public WorkloadIdentityRESTCatalog() {
        super(config -> new BearerInjectingRESTClient(
            HTTPClient.builder(config)
                .uri(config.get(CatalogProperties.URI))
                .withHeaders(RESTUtil.extractPrefixMap(config, "header."))
                .build(),
            tokenSupplier(config)));
    }

    /** Whether a bearer-token supplier can be resolved from the given catalog config / environment. */
    public static boolean canSupplyToken(Map<String, String> config) {
        String staticToken = config.get(TOKEN_PROP);
        if (staticToken != null && !staticToken.isEmpty()) {
            return true;
        }
        if (WorkloadIdentityTokenProvider.isAvailable()) {
            return true;
        }
        String env = System.getenv(TOKEN_ENV);
        return env != null && !env.isEmpty();
    }

    public static Supplier<String> tokenSupplier(Map<String, String> config) {
        String staticToken = config.get(TOKEN_PROP);
        if (staticToken != null && !staticToken.isEmpty()) {
            return () -> staticToken;
        }
        if (WorkloadIdentityTokenProvider.isAvailable()) {
            String scope = config.get(TOKEN_SCOPE_PROP);
            if (scope == null || scope.isEmpty()) {
                scope = System.getenv(TOKEN_SCOPE_ENV);
            }
            if (scope == null || scope.isEmpty()) {
                throw new IllegalStateException("Azure Workload Identity requires a token scope: set '"
                    + TOKEN_SCOPE_PROP + "' or env " + TOKEN_SCOPE_ENV);
            }
            return WorkloadIdentityTokenProvider.fromEnvironment(scope);
        }
        return () -> {
            String env = System.getenv(TOKEN_ENV);
            if (env == null || env.isEmpty()) {
                throw new IllegalStateException("No REST catalog token: set '" + TOKEN_PROP
                    + "', configure Azure Workload Identity, or set env " + TOKEN_ENV);
            }
            return env;
        };
    }

    /**
     * Delegates to a real {@link RESTClient} but overrides the {@code Authorization} header on every request with a
     * freshly-supplied bearer token. Only the 5 abstract {@link RESTClient} methods need overriding; the interface's
     * default methods funnel through them.
     */
    static final class BearerInjectingRESTClient implements RESTClient {
        private final RESTClient delegate;
        private final Supplier<String> token;

        BearerInjectingRESTClient(RESTClient delegate, Supplier<String> token) {
            this.delegate = delegate;
            this.token = token;
        }

        private Map<String, String> auth(Map<String, String> headers) {
            Map<String, String> merged = new HashMap<>();
            if (headers != null) {
                merged.putAll(headers);
            }
            merged.put("Authorization", "Bearer " + token.get());
            return merged;
        }

        @Override
        public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            delegate.head(path, auth(headers), errorHandler);
        }

        @Override
        public <T extends RESTResponse> T delete(String path, Class<T> responseType, Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler) {
            return delegate.delete(path, responseType, auth(headers), errorHandler);
        }

        @Override
        public <T extends RESTResponse> T get(String path, Map<String, String> queryParams, Class<T> responseType,
            Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            return delegate.get(path, queryParams, responseType, auth(headers), errorHandler);
        }

        @Override
        public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
            Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            return delegate.post(path, body, responseType, auth(headers), errorHandler);
        }

        @Override
        public <T extends RESTResponse> T postForm(String path, Map<String, String> formData, Class<T> responseType,
            Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            return delegate.postForm(path, formData, responseType, auth(headers), errorHandler);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
