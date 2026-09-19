package com.tushar.sentinel.service;

import java.math.BigDecimal;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** The rule book. Amounts arrive already converted to base currency by the ingestion service. */
@ConfigurationProperties(prefix = "sentinel")
public record SentinelProperties(List<Rule> rules) {

    /**
     * One detection rule, entirely from YAML. Adding a rule is a new list entry:
     * no Java change, no redeployment.
     *
     * <p>{@code condition} and {@code filter} are SpEL expressions evaluated against a
     * transaction, so its properties are referenced by name (for example {@code amountBase}).
     */
    public record Rule(
            String code,
            String typology,
            boolean enabled,
            int weight,
            Type type,
            Scope scope,
            String condition,
            String filter,
            BigDecimal threshold,
            int windowHours,
            int baselineDays,
            int minBaselineTxns,
            String explanation) {
    }

    /** Shapes of detection logic. A new rule reuses one; only its parameters differ. */
    public enum Type {
        SINGLE_TRANSACTION,
        WINDOWED_COUNT,
        INFLOW_OUTFLOW_RATIO,
        BASELINE_DEVIATION
    }

    public enum Scope {
        ACCOUNT,
        CUSTOMER
    }
}
