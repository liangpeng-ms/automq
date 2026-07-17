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

import java.util.Map;
import java.util.function.Supplier;

/**
 * Resolves an Entra ID (AAD) bearer token in a 3-tier order shared by the AutoMQ HDFS backend and the Iceberg REST
 * catalog auth ({@link WorkloadIdentityAuthManager}):
 * <ol>
 *   <li>static {@code token} property (dev/tests);</li>
 *   <li>Azure Workload Identity (auto-refreshing), scope from {@code token.scope} or env {@code HDFS_TOKEN_SCOPE};</li>
 *   <li>{@code AAD_TOKEN} env var (non-refreshing fallback).</li>
 * </ol>
 */
public final class WorkloadIdentityTokens {
    public static final String TOKEN_PROP = "token";
    public static final String TOKEN_SCOPE_PROP = "token.scope";

    private WorkloadIdentityTokens() {
    }

    /** Whether a bearer-token supplier can be resolved from the given config / environment. */
    public static boolean canSupplyToken(Map<String, String> config) {
        return com.automq.stream.s3.operator.WorkloadIdentityTokens.canSupply(config.get(TOKEN_PROP));
    }

    public static Supplier<String> tokenSupplier(Map<String, String> config) {
        return com.automq.stream.s3.operator.WorkloadIdentityTokens.resolve(
            config.get(TOKEN_PROP), config.get(TOKEN_SCOPE_PROP), "REST catalog token");
    }
}
