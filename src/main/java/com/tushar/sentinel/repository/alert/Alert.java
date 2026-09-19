package com.tushar.sentinel.repository.alert;

import com.tushar.sentinel.repository.account.Account;
import com.tushar.sentinel.repository.customer.Customer;
import com.tushar.sentinel.repository.txn.Transaction;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/** A detection rule firing, with the evidence and explanation an analyst needs to act. */
@Entity
@Table(name = "alert")
@Getter
@Setter
@NoArgsConstructor
public class Alert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "alert_ref", nullable = false, unique = true, length = 48)
    private String alertRef;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "customer_id", nullable = false)
    private Customer customer;

    /** Null for customer-level rules that span more than one account. */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "account_id")
    private Account account;

    @Column(name = "rule_code", nullable = false, length = 48)
    private String ruleCode;

    @Column(nullable = false, length = 64)
    private String typology;

    @Column(name = "risk_score", nullable = false)
    private int riskScore;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Severity severity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private AlertStatus status;

    /** Plain-English reason the rule fired; the regulator-facing justification. */
    @Column(nullable = false, length = 2000)
    private String explanation;

    /**
     * Natural key of the underlying pattern. Unique in the database, so two threads
     * evaluating the same account concurrently cannot create sibling alerts.
     */
    @Column(name = "dedup_key", nullable = false, unique = true, length = 160)
    private String dedupKey;

    /** Transactions that caused this alert to fire. */
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "alert_evidence",
            joinColumns = @JoinColumn(name = "alert_id"),
            inverseJoinColumns = @JoinColumn(name = "txn_id"))
    private Set<Transaction> evidence = new LinkedHashSet<>();

    @Column(name = "detected_at", nullable = false)
    private Instant detectedAt;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
