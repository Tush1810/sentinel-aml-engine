package com.tushar.sentinel.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tushar.sentinel.repository.txn.Transaction;
import com.tushar.sentinel.repository.txn.TransactionRepository;
import com.tushar.sentinel.service.detection.DetectionEngine;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * The consumer's only real decision is which change events deserve an evaluation.
 * Everything else is delegated, so that decision is what these cover.
 */
@ExtendWith(MockitoExtension.class)
class TransactionChangeListenerTest {

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private DetectionEngine detectionEngine;

    private TransactionChangeListener listener;

    @BeforeEach
    void setUp() {
        listener = new TransactionChangeListener(new ObjectMapper(), transactionRepository, detectionEngine);
    }

    private static String envelope(String op, String after) {
        return "{\"before\":null,\"after\":" + after + ",\"op\":\"" + op + "\",\"ts_ms\":1}";
    }

    private void expectEvaluationOf(long id) {
        Transaction transaction = new Transaction();
        transaction.setTxnRef("TXN_" + id);
        when(transactionRepository.findById(id)).thenReturn(Optional.of(transaction));
        when(detectionEngine.evaluate(transaction)).thenReturn(List.of());
    }

    @Test
    void evaluatesAnInsert() {
        expectEvaluationOf(42L);

        listener.onChange(envelope("c", "{\"id\":42,\"txn_ref\":\"TXN_42\"}"));

        verify(detectionEngine).evaluate(any(Transaction.class));
    }

    /** Debezium emits op=r for every row of the initial snapshot; those need evaluating too. */
    @Test
    void evaluatesASnapshotRead() {
        expectEvaluationOf(7L);

        listener.onChange(envelope("r", "{\"id\":7}"));

        verify(detectionEngine).evaluate(any(Transaction.class));
    }

    /** Transactions are never updated, so anything but an insert is a no-op. */
    @Test
    void ignoresUpdatesAndDeletes() {
        listener.onChange(envelope("u", "{\"id\":42}"));
        listener.onChange(envelope("d", "null"));

        verifyNoInteractions(transactionRepository, detectionEngine);
    }

    /** A delete tombstone arrives as a null value. */
    @Test
    void ignoresTombstones() {
        listener.onChange(null);

        verifyNoInteractions(transactionRepository, detectionEngine);
    }

    /** The connector may be configured with schemas enabled, which nests the envelope. */
    @Test
    void readsTheEnvelopeWhenTheConnectorSendsSchemas() {
        expectEvaluationOf(99L);

        listener.onChange("{\"schema\":{\"type\":\"struct\"},\"payload\":"
                + envelope("c", "{\"id\":99}") + "}");

        verify(detectionEngine).evaluate(any(Transaction.class));
    }

    @Test
    void skipsUnreadableAndIncompleteEvents() {
        listener.onChange("not json at all");
        listener.onChange(envelope("c", "{\"txn_ref\":\"TXN_1\"}"));

        verifyNoInteractions(transactionRepository, detectionEngine);
    }

    /** The row is gone or not visible: log and move on rather than failing the record forever. */
    @Test
    void skipsWhenTheRowIsMissing() {
        when(transactionRepository.findById(5L)).thenReturn(Optional.empty());

        listener.onChange(envelope("c", "{\"id\":5}"));

        verify(detectionEngine, never()).evaluate(any());
    }
}
