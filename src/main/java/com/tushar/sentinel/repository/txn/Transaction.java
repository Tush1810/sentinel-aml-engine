package com.tushar.sentinel.repository.txn;

import com.tushar.sentinel.repository.account.Account;
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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

/**
 * A single movement of money on an {@link Account}. Table is named {@code txn} because
 * {@code transaction} is a SQL reserved word.
 */
@Entity
@Table(name = "txn")
@Getter
@Setter
@NoArgsConstructor
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "txn_ref", nullable = false, unique = true, length = 48)
    private String txnRef;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "account_id", nullable = false)
    private Account account;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 8)
    private TxnDirection direction;

    @Enumerated(EnumType.STRING)
    @Column(name = "txn_type", nullable = false, length = 32)
    private TxnType txnType;

    /** Amount as transacted, in {@link #currency}. */
    @Column(nullable = false, precision = 18, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    /** Amount normalized to the base currency; every threshold rule compares against this. */
    @Column(name = "amount_base", nullable = false, precision = 18, scale = 2)
    private BigDecimal amountBase;

    /** Rate applied to reach {@link #amountBase}; stored so an alert stays reproducible. */
    @Column(name = "exchange_rate", nullable = false, precision = 18, scale = 6)
    private BigDecimal exchangeRate;

    @Column(name = "counterparty_name", length = 160)
    private String counterpartyName;

    @Column(name = "counterparty_account", length = 48)
    private String counterpartyAccount;

    @Column(name = "counterparty_bank", length = 160)
    private String counterpartyBank;

    /** ISO country of the counterparty; drives high-risk jurisdiction detection. */
    @Column(name = "counterparty_country", length = 2)
    private String counterpartyCountry;

    @Column(length = 32)
    private String channel;

    @Column(length = 255)
    private String description;

    /** When the transaction happened. Detection windows use this, never arrival time. */
    @Column(name = "txn_timestamp", nullable = false)
    private Instant txnTimestamp;

    /** When Sentinel received it; differs from {@link #txnTimestamp} for late-arriving data. */
    @CreationTimestamp
    @Column(name = "ingested_at", nullable = false, updatable = false)
    private Instant ingestedAt;
}
