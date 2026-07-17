package com.automq.stream.s3.operator;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadIdentityTokenUtilTest {

    @Test
    void staticTokenTakesPrecedence() {
        Supplier<String> s = WorkloadIdentityTokenUtil.tokenSupplier("static-abc", null, "WebHDFS token");
        assertEquals("static-abc", s.get());
    }

    @Test
    void canSupply_trueForNonEmptyStaticToken() {
        assertTrue(WorkloadIdentityTokenUtil.canSupply("static-abc"));
    }

    @Test
    void canSupply_falseForEmptyStaticTokenWithoutEnv() {
        // Assumes the test JVM has neither Workload Identity env nor AAD_TOKEN set.
        assertFalse(WorkloadIdentityTokenUtil.canSupply(""));
        assertFalse(WorkloadIdentityTokenUtil.canSupply(null));
    }
}
