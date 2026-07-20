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

package com.automq.hdfs.token;

import org.apache.commons.lang3.StringUtils;

import java.util.function.Supplier;

/**
 * Builds an Entra ID (AAD) bearer-token supplier using a 3-tier resolution order shared by the AutoMQ HDFS/WebHDFS
 * backends and the Iceberg REST catalog auth. Azure Workload Identity is the primary source; the other two tiers are
 * escape hatches for when it is not configured:
 * <ol>
 *   <li>a static token (dev/tests) takes precedence;</li>
 *   <li>Azure Workload Identity (auto-refreshing), scope from the caller-provided value else env {@code HDFS_TOKEN_SCOPE};</li>
 *   <li>the {@code AAD_TOKEN} env var (non-refreshing fallback).</li>
 * </ol>
 * Callers pass already-extracted values, so this class does not depend on any particular config carrier
 * ({@code Map}, {@code BucketURI}, ...).
 */
public final class WorkloadIdentityTokenUtil {
    private static final String TOKEN_ENV = "AAD_TOKEN";
    private static final String TOKEN_SCOPE_ENV = "HDFS_TOKEN_SCOPE";

    private WorkloadIdentityTokenUtil() {
    }

    /** Whether a bearer-token supplier can be resolved from the given static token / current environment. */
    public static boolean canSupply(String staticToken) {
        if (StringUtils.isNotEmpty(staticToken)) {
            return true;
        }
        if (WorkloadIdentityTokenProvider.isAvailable()) {
            return true;
        }
        return StringUtils.isNotEmpty(System.getenv(TOKEN_ENV));
    }

    /**
     * @param staticToken   an explicit static token, or null/empty to fall through
     * @param explicitScope the Workload Identity scope from caller config, or null/empty to fall back to env
     * @param errCtx        a short noun phrase used only in the "no token" error message
     */
    public static Supplier<String> tokenSupplier(String staticToken, String explicitScope, String errCtx) {
        if (StringUtils.isNotEmpty(staticToken)) {
            return () -> staticToken;
        }
        if (WorkloadIdentityTokenProvider.isAvailable()) {
            String scope = StringUtils.isNotEmpty(explicitScope) ? explicitScope : System.getenv(TOKEN_SCOPE_ENV);
            if (StringUtils.isEmpty(scope)) {
                throw new IllegalStateException(
                    "Azure Workload Identity requires a token scope: set an explicit scope or env " + TOKEN_SCOPE_ENV);
            }
            return WorkloadIdentityTokenProvider.fromEnvironment(scope);
        }
        return () -> {
            String env = System.getenv(TOKEN_ENV);
            if (StringUtils.isEmpty(env)) {
                throw new IllegalStateException("No " + errCtx + ": set a static token, configure Azure Workload "
                    + "Identity, or set env " + TOKEN_ENV);
            }
            return env;
        };
    }
}
