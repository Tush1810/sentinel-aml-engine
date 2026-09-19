package com.tushar.sentinel.repository.account;

import com.tushar.sentinel.repository.customer.Customer;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/** Account held by a {@link Customer}; transactions hang off it. */
@Entity
@Table(name = "account")
@Getter
@Setter
@NoArgsConstructor
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Business identifier from the source system, e.g. ACC_000001. */
    @Column(name = "account_ref", nullable = false, unique = true, length = 32)
    private String accountRef;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "customer_id", nullable = false)
    private Customer customer;

    /** SAVINGS, CURRENT, NRE and similar; kept as text so ingestion tolerates new types. */
    @Column(name = "account_type", nullable = false, length = 32)
    private String accountType;

    @Enumerated(EnumType.STRING)
    @Column(name = "account_status", nullable = false, length = 16)
    private AccountStatus accountStatus;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "open_date")
    private LocalDate openDate;

    @Column(name = "close_date")
    private LocalDate closeDate;

    @Column(name = "branch_code", length = 16)
    private String branchCode;

    @Column(name = "branch_city", length = 100)
    private String branchCity;

    @Column(name = "current_balance", precision = 18, scale = 2)
    private BigDecimal currentBalance;

    @Column(name = "avg_monthly_balance_6m", precision = 18, scale = 2)
    private BigDecimal avgMonthlyBalance6m;

    @Column(name = "credit_limit", precision = 18, scale = 2)
    private BigDecimal creditLimit;

    @Column(name = "credit_utilization_pct", precision = 6, scale = 2)
    private BigDecimal creditUtilizationPct;

    @Column(name = "overdraft_enabled", nullable = false)
    private boolean overdraftEnabled;

    @Column(name = "card_type", length = 32)
    private String cardType;

    @Column(name = "joint_account", nullable = false)
    private boolean jointAccount;

    @Column(name = "num_linked_devices")
    private Integer numLinkedDevices;

    @Column(name = "mobile_banking_enrolled", nullable = false)
    private boolean mobileBankingEnrolled;

    @Column(name = "last_login_date")
    private LocalDate lastLoginDate;

    @Column(name = "avg_monthly_txn_count")
    private Integer avgMonthlyTxnCount;

    @Column(name = "account_tier", length = 32)
    private String accountTier;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
