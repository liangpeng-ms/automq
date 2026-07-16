package kafka.automq.table;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadIdentityTokensTest {
    @Test
    void staticTokenTakesPrecedence() {
        Map<String, String> config = Map.of(WorkloadIdentityTokens.TOKEN_PROP, "static-abc");
        assertTrue(WorkloadIdentityTokens.canSupplyToken(config));
        Supplier<String> s = WorkloadIdentityTokens.tokenSupplier(config);
        assertEquals("static-abc", s.get());
    }

    @Test
    void noTokenAndNoWorkloadIdentityCannotSupply() {
        // Assumes the test JVM has neither a Workload Identity federated env nor AAD_TOKEN set.
        assertFalse(WorkloadIdentityTokens.canSupplyToken(Map.of()));
    }
}
