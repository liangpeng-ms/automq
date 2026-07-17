package com.automq.stream.s3.operator;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadIdentityTokensTest {

    @Test
    void staticTokenTakesPrecedence() {
        Supplier<String> s = WorkloadIdentityTokens.resolve("static-abc", null, "WebHDFS token");
        assertEquals("static-abc", s.get());
    }

    @Test
    void canSupply_trueForNonEmptyStaticToken() {
        assertTrue(WorkloadIdentityTokens.canSupply("static-abc"));
    }

    @Test
    void canSupply_falseForEmptyStaticTokenWithoutEnv() {
        // Assumes the test JVM has neither Workload Identity env nor AAD_TOKEN set.
        assertFalse(WorkloadIdentityTokens.canSupply(""));
        assertFalse(WorkloadIdentityTokens.canSupply(null));
    }
}
