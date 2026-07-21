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

package com.automq.hdfs.common.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;

import org.apache.commons.lang3.StringUtils;

import java.util.function.Supplier;

/**
 * Supplies Entra ID (Azure AD) bearer tokens using
 * <a href="https://learn.microsoft.com/azure/aks/workload-identity-overview">Azure Workload Identity</a> via the
 * {@code azure-identity} SDK, so no long-lived secret has to be provisioned into the broker.
 * <p>
 * {@link com.azure.identity.WorkloadIdentityCredential} reads the standard environment injected by the workload-identity
 * admission webhook ({@code AZURE_CLIENT_ID}, {@code AZURE_TENANT_ID}, {@code AZURE_AUTHORITY_HOST} and the projected
 * {@code AZURE_FEDERATED_TOKEN_FILE}) and exchanges the projected service-account token for an access token.
 * <p>
 * No caching is done here: the credential caches the {@link AccessToken} internally and refreshes it shortly before
 * expiry, re-reading the rotated projected token automatically, so {@link #get()} simply delegates to it.
 */
public class WorkloadIdentityTokenProvider implements Supplier<String> {
    private final TokenCredential credential;
    private final TokenRequestContext requestContext;
    private final String scope;

    public WorkloadIdentityTokenProvider(String scope) {
        this(new WorkloadIdentityCredentialBuilder()
            // Force the JDK HttpClient so azure-identity does not load the netty-based
            // provider (which clashes with Kafka's bundled netty at runtime).
            .httpClient(new JdkHttpClientBuilder().build())
            .build(), scope);
    }

    WorkloadIdentityTokenProvider(TokenCredential credential, String scope) {
        this.credential = credential;
        this.scope = scope;
        this.requestContext = new TokenRequestContext().addScopes(scope);
    }

    /** Whether the pod is running with Azure Workload Identity federation configured. */
    public static boolean isAvailable() {
        return StringUtils.isNotBlank(System.getenv("AZURE_FEDERATED_TOKEN_FILE"))
            && StringUtils.isNotBlank(System.getenv("AZURE_CLIENT_ID"))
            && StringUtils.isNotBlank(System.getenv("AZURE_TENANT_ID"));
    }

    public static WorkloadIdentityTokenProvider fromEnvironment(String scope) {
        return new WorkloadIdentityTokenProvider(scope);
    }

    @Override
    public String get() {
        AccessToken token = credential.getTokenSync(requestContext);
        if (token == null || StringUtils.isBlank(token.getToken())) {
            throw new IllegalStateException("Workload Identity returned no access token for scope " + scope);
        }
        return token.getToken();
    }
}
