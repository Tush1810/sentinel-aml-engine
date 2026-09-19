package com.tushar.sentinel.service.detection;

import com.tushar.sentinel.repository.alert.Alert;
import com.tushar.sentinel.repository.alert.AlertRepository;
import com.tushar.sentinel.repository.alert.AlertStatus;
import com.tushar.sentinel.repository.alert.Severity;
import com.tushar.sentinel.repository.customer.Customer;
import com.tushar.sentinel.repository.customer.KycStatus;
import com.tushar.sentinel.repository.customer.RiskRating;
import com.tushar.sentinel.repository.txn.Transaction;
import com.tushar.sentinel.service.SentinelProperties.Rule;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Scores a rule firing and either raises an alert or folds it into the one already
 * representing that pattern, so a single typology cannot produce dozens of alerts.
 */
@Service
public class AlertService {

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    private static final int MAX_SCORE = 100;
    private static final int PEP_UPLIFT = 15;
    private static final int HIGH_RISK_UPLIFT = 10;
    private static final int MEDIUM_RISK_UPLIFT = 5;
    private static final int UNVERIFIED_KYC_UPLIFT = 10;
    private static final int CRITICAL_FROM = 85;
    private static final int HIGH_FROM = 70;
    private static final int MEDIUM_FROM = 50;

    private final AlertRepository alertRepository;

    public AlertService(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    public Alert raise(Rule rule, String explanation, List<Transaction> evidence,
                       String dedupKey, Transaction subject) {
        Customer customer = subject.getAccount().getCustomer();
        int score = score(rule.weight(), customer);

        Alert alert = alertRepository.findByDedupKey(dedupKey).orElseGet(() -> newAlert(dedupKey, customer));
        alert.setRuleCode(rule.code());
        alert.setTypology(rule.typology());
        alert.setExplanation(explanation);
        alert.setRiskScore(score);
        alert.setSeverity(severityOf(score));
        alert.setAccount(subject.getAccount());
        alert.getEvidence().addAll(evidence);

        Alert saved = alertRepository.save(alert);
        log.info("Alert {} for rule {} on customer {}; score={}",
                saved.getAlertRef(), rule.code(), customer.getCustomerRef(), score);
        return saved;
    }

    private Alert newAlert(String dedupKey, Customer customer) {
        Alert alert = new Alert();
        alert.setAlertRef("ALT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        alert.setDedupKey(dedupKey);
        alert.setCustomer(customer);
        alert.setStatus(AlertStatus.OPEN);
        alert.setDetectedAt(Instant.now());
        return alert;
    }

    /** A PEP structuring funds is more urgent than an ordinary customer doing the same. */
    private int score(int ruleWeight, Customer customer) {
        int score = ruleWeight;
        if (customer.isPoliticallyExposed()) {
            score += PEP_UPLIFT;
        }
        if (customer.getRiskRating() == RiskRating.HIGH) {
            score += HIGH_RISK_UPLIFT;
        } else if (customer.getRiskRating() == RiskRating.MEDIUM) {
            score += MEDIUM_RISK_UPLIFT;
        }
        if (customer.getKycStatus() != KycStatus.VERIFIED) {
            score += UNVERIFIED_KYC_UPLIFT;
        }
        return Math.min(score, MAX_SCORE);
    }

    private Severity severityOf(int score) {
        if (score >= CRITICAL_FROM) {
            return Severity.CRITICAL;
        }
        if (score >= HIGH_FROM) {
            return Severity.HIGH;
        }
        if (score >= MEDIUM_FROM) {
            return Severity.MEDIUM;
        }
        return Severity.LOW;
    }
}
