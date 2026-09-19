package com.tushar.sentinel.repository.txn;

import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface TransactionRepository extends JpaRepository<Transaction, Long> {

    /** Window query behind the structuring and rapid-movement rules. */
    List<Transaction> findByAccountIdAndTxnTimestampBetweenOrderByTxnTimestampAsc(
            Long accountId, Instant from, Instant to);

    /** Customer-level window, used by behavioural deviation across all of a customer's accounts. */
    @Query("""
            select t from Transaction t
            where t.account.customer.id = :customerId
              and t.txnTimestamp between :from and :to
            order by t.txnTimestamp asc
            """)
    List<Transaction> findByCustomerIdAndWindow(Long customerId, Instant from, Instant to);
}
