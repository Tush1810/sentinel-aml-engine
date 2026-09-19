package com.tushar.sentinel.service.detection;

import com.tushar.sentinel.repository.alert.Alert;
import com.tushar.sentinel.repository.txn.Transaction;
import com.tushar.sentinel.repository.txn.TransactionRepository;
import com.tushar.sentinel.repository.txn.TxnDirection;
import com.tushar.sentinel.service.SentinelProperties;
import com.tushar.sentinel.service.SentinelProperties.Rule;
import com.tushar.sentinel.service.SentinelProperties.Scope;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Service;

/**
 * Evaluates every enabled rule against a transaction. Each rule is a YAML entry naming one
 * of four shapes, so a new typology is configuration rather than code.
 */
@Service
public class DetectionEngine {

    private static final Logger log = LoggerFactory.getLogger(DetectionEngine.class);
    private static final int PERCENT = 100;
    private static final int SCALE = 2;

    private final SentinelProperties properties;
    private final TransactionRepository transactionRepository;
    private final AlertService alertService;
    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final Map<String, Expression> expressions = new ConcurrentHashMap<>();

    public DetectionEngine(
            SentinelProperties properties,
            TransactionRepository transactionRepository,
            AlertService alertService) {
        this.properties = properties;
        this.transactionRepository = transactionRepository;
        this.alertService = alertService;
    }

    public List<Alert> evaluate(Transaction transaction) {
        List<Alert> raised = new ArrayList<>();
        for (Rule rule : properties.rules()) {
            if (!rule.enabled()) {
                continue;
            }
            Optional<Hit> hit = switch (rule.type()) {
                case SINGLE_TRANSACTION -> singleTransaction(rule, transaction);
                case WINDOWED_COUNT -> windowedCount(rule, transaction);
                case INFLOW_OUTFLOW_RATIO -> inflowOutflowRatio(rule, transaction);
                case BASELINE_DEVIATION -> baselineDeviation(rule, transaction);
            };
            hit.ifPresent(value -> raised.add(alertService.raise(rule, value.explanation(),
                    value.evidence(), value.dedupKey(), transaction)));
        }
        return raised;
    }

    /** Threshold and jurisdiction rules: the configured condition tested against this transaction. */
    private Optional<Hit> singleTransaction(Rule rule, Transaction transaction) {
        if (!matches(rule.condition(), transaction)) {
            return Optional.empty();
        }
        String explanation = render(rule.explanation(), Map.of(
                "amount", transaction.getAmountBase(),
                "account", transaction.getAccount().getAccountRef(),
                "country", String.valueOf(transaction.getCounterpartyCountry()),
                "threshold", String.valueOf(rule.threshold())));
        return Optional.of(new Hit(explanation, List.of(transaction),
                rule.code() + ":" + transaction.getTxnRef()));
    }

    /** Structuring and round-number: count transactions in the window matching the filter. */
    private Optional<Hit> windowedCount(Rule rule, Transaction transaction) {
        if (!matches(rule.filter(), transaction)) {
            return Optional.empty();
        }
        List<Transaction> matching = window(rule, transaction).stream()
                .filter(candidate -> matches(rule.filter(), candidate))
                .toList();
        if (BigDecimal.valueOf(matching.size()).compareTo(rule.threshold()) < 0) {
            return Optional.empty();
        }
        String explanation = render(rule.explanation(), Map.of(
                "count", matching.size(),
                "total", sum(matching),
                "window", rule.windowHours(),
                "account", transaction.getAccount().getAccountRef()));
        return Optional.of(new Hit(explanation, matching, dedupKey(rule, transaction)));
    }

    /**
     * Rapid movement: money that does not rest. Outflow measured against inflow over the window.
     *
     * <p>ponytail: aggregate in-vs-out over the window, not deposit-to-withdrawal matching, so
     * outflow funded by an existing balance can read above 100%. Match per deposit if the false
     * positives matter.
     */
    private Optional<Hit> inflowOutflowRatio(Rule rule, Transaction transaction) {
        List<Transaction> window = window(rule, transaction);
        BigDecimal inflow = sum(window.stream().filter(t -> t.getDirection() == TxnDirection.CREDIT).toList());
        BigDecimal outflow = sum(window.stream().filter(t -> t.getDirection() == TxnDirection.DEBIT).toList());
        if (inflow.signum() <= 0) {
            return Optional.empty();
        }
        BigDecimal ratio = outflow.multiply(BigDecimal.valueOf(PERCENT)).divide(inflow, 0, RoundingMode.DOWN);
        if (ratio.compareTo(rule.threshold()) < 0) {
            return Optional.empty();
        }
        String explanation = render(rule.explanation(), Map.of(
                "inflow", inflow, "outflow", outflow, "ratio", ratio,
                "window", rule.windowHours(),
                "account", transaction.getAccount().getAccountRef()));
        return Optional.of(new Hit(explanation, window, dedupKey(rule, transaction)));
    }

    /**
     * Behavioural deviation: recent activity against the customer's own rolling average.
     * A customer without enough history has no meaningful baseline, so the rule needs a
     * minimum transaction count before it can fire.
     */
    private Optional<Hit> baselineDeviation(Rule rule, Transaction transaction) {
        List<Transaction> recent = window(rule, transaction);
        Instant baselineEnd = transaction.getTxnTimestamp().minus(Duration.ofHours(rule.windowHours()));
        Instant baselineStart = baselineEnd.minus(Duration.ofDays(rule.baselineDays()));
        List<Transaction> baseline = transactionRepository.findByCustomerIdAndWindow(
                customerId(transaction), baselineStart, baselineEnd);

        if (baseline.size() < rule.minBaselineTxns()) {
            return Optional.empty();
        }
        BigDecimal dailyAverage = sum(baseline)
                .divide(BigDecimal.valueOf(rule.baselineDays()), SCALE, RoundingMode.HALF_UP);
        BigDecimal actual = sum(recent);
        if (dailyAverage.signum() <= 0 || actual.compareTo(dailyAverage.multiply(rule.threshold())) <= 0) {
            return Optional.empty();
        }
        String explanation = render(rule.explanation(), Map.of(
                "actual", actual, "average", dailyAverage, "multiplier", rule.threshold(),
                "baselineDays", rule.baselineDays(),
                "customer", transaction.getAccount().getCustomer().getCustomerRef()));
        return Optional.of(new Hit(explanation, recent, dedupKey(rule, transaction)));
    }

    private List<Transaction> window(Rule rule, Transaction transaction) {
        Instant end = transaction.getTxnTimestamp();
        Instant start = end.minus(Duration.ofHours(rule.windowHours()));
        if (rule.scope() == Scope.CUSTOMER) {
            return transactionRepository.findByCustomerIdAndWindow(customerId(transaction), start, end);
        }
        return transactionRepository.findByAccountIdAndTxnTimestampBetweenOrderByTxnTimestampAsc(
                transaction.getAccount().getId(), start, end);
    }

    /** Bucketing by day means later transactions in one pattern update a single alert. */
    private String dedupKey(Rule rule, Transaction transaction) {
        String scopeRef = rule.scope() == Scope.CUSTOMER
                ? transaction.getAccount().getCustomer().getCustomerRef()
                : transaction.getAccount().getAccountRef();
        return rule.code() + ":" + scopeRef + ":" + transaction.getTxnTimestamp().truncatedTo(ChronoUnit.DAYS);
    }

    private boolean matches(String expression, Transaction transaction) {
        Expression parsed = expressions.computeIfAbsent(expression, parser::parseExpression);
        return Boolean.TRUE.equals(
                parsed.getValue(new StandardEvaluationContext(transaction), Boolean.class));
    }

    private String render(String template, Map<String, Object> values) {
        String rendered = template;
        for (Map.Entry<String, Object> value : values.entrySet()) {
            rendered = rendered.replace("{" + value.getKey() + "}", String.valueOf(value.getValue()));
        }
        return rendered;
    }

    private BigDecimal sum(List<Transaction> transactions) {
        return transactions.stream().map(Transaction::getAmountBase).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private Long customerId(Transaction transaction) {
        return transaction.getAccount().getCustomer().getId();
    }

    private record Hit(String explanation, List<Transaction> evidence, String dedupKey) {
    }
}
