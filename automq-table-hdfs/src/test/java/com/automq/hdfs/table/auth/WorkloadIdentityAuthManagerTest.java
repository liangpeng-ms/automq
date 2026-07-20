package com.automq.hdfs.table.auth;

import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadIdentityAuthManagerTest {

    private static HTTPRequest emptyRequest() {
        return ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost:9001"))
            .method(HTTPMethod.GET)
            .path("v1/config")
            .build();
    }

    @Test
    void injectsBearerFromStaticToken() {
        AuthManager mgr = new WorkloadIdentityAuthManager("test");
        AuthSession session = mgr.catalogSession(null, Map.of(WorkloadIdentityAuthManager.TOKEN_PROP, "tkn-123"));
        HTTPRequest authed = session.authenticate(emptyRequest());
        assertTrue(authed.headers().contains("Authorization"));
        assertEquals("Bearer tkn-123",
            authed.headers().entries("Authorization").iterator().next().value());
    }

    @Test
    void canSupplyToken_trueForStaticToken() {
        assertTrue(WorkloadIdentityAuthManager.canSupplyToken(Map.of(WorkloadIdentityAuthManager.TOKEN_PROP, "static-abc")));
    }

    @Test
    void canSupplyToken_falseWithoutTokenOrWorkloadIdentity() {
        // Assumes the test JVM has neither a Workload Identity federated env nor AAD_TOKEN set.
        assertFalse(WorkloadIdentityAuthManager.canSupplyToken(Map.of()));
    }

    @Test
    void loadableViaRestAuthTypeSpi() {
        AuthManager mgr = AuthManagers.loadAuthManager(
            "test", Map.of("rest.auth.type", "com.automq.hdfs.table.auth.WorkloadIdentityAuthManager"));
        assertTrue(mgr instanceof WorkloadIdentityAuthManager);
        mgr.close();
    }
}
