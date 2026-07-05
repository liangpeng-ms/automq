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

package com.automq.stream.s3.operator;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link WorkloadIdentityTokenProvider}. The azure-identity token exchange is stubbed via a fake
 * {@link TokenCredential} so the tests need no Entra ID connectivity. Token caching/refresh is owned by the credential,
 * so it is not re-tested here.
 */
@Tag("S3Unit")
public class WorkloadIdentityTokenProviderTest {

    @Test
    public void returnsTokenFromCredentialForRequestedScope() {
        FakeCredential credential = new FakeCredential(3600);
        WorkloadIdentityTokenProvider provider =
            new WorkloadIdentityTokenProvider(credential, "api://resource-x/.default");

        assertEquals("tok-1", provider.get());
        assertEquals("api://resource-x/.default", credential.lastScope);
    }

    @Test
    public void delegatesEachCallToCredential() {
        // The provider adds no caching of its own; the credential owns token caching and refresh.
        FakeCredential credential = new FakeCredential(3600);
        WorkloadIdentityTokenProvider provider =
            new WorkloadIdentityTokenProvider(credential, "api://resource-x/.default");

        assertEquals("tok-1", provider.get());
        assertEquals("tok-2", provider.get());
        assertEquals(2, credential.issued.get());
    }

    @Test
    public void throwsWhenCredentialReturnsNoToken() {
        FakeCredential credential = new FakeCredential(3600);
        credential.returnNull = true;
        WorkloadIdentityTokenProvider provider =
            new WorkloadIdentityTokenProvider(credential, "api://resource-x/.default");

        assertThrows(IllegalStateException.class, provider::get);
    }

    /** Stubs the azure-identity credential, returning synthetic tokens and recording the requested scope. */
    private static final class FakeCredential implements TokenCredential {
        private final AtomicInteger issued = new AtomicInteger();
        private final long lifetimeSeconds;
        private volatile String lastScope;
        private volatile boolean returnNull;

        private FakeCredential(long lifetimeSeconds) {
            this.lifetimeSeconds = lifetimeSeconds;
        }

        @Override
        public Mono<AccessToken> getToken(TokenRequestContext request) {
            lastScope = request.getScopes().get(0);
            if (returnNull) {
                return Mono.empty();
            }
            String token = "tok-" + issued.incrementAndGet();
            return Mono.just(new AccessToken(token, OffsetDateTime.now().plusSeconds(lifetimeSeconds)));
        }
    }
}
