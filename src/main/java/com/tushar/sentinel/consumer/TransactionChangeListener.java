package com.tushar.sentinel.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tushar.sentinel.repository.txn.Transaction;
import com.tushar.sentinel.service.detection.DetectionEngine;
import com.tushar.sentinel.repository.txn.TransactionRepository;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes row-level change events for the transaction table and runs the rule book against
 * each new row. The event is only a notification that a row exists: detection needs the
 * customer, the account and the surrounding window, so the transaction is re-read from the
 * database rather than rebuilt from the change payload.
 *
 * <p>The events are produced by a Debezium Postgres connector reading the write-ahead log, so
 * a row cannot be committed without its event being published. That is the reason for reading
 * changes rather than having the ingestion service publish its own event: there is no window
 * in which a transaction is stored but never evaluated.
 */
@Component
public class TransactionChangeListener {

    private static final Logger log = LoggerFactory.getLogger(TransactionChangeListener.class);

    /** Debezium operations worth evaluating: {@code c} is an insert, {@code r} a snapshot read. */
    private static final Set<String> EVALUATED_OPS = Set.of("c", "r");

    private final ObjectMapper objectMapper;
    private final TransactionRepository transactionRepository;
    private final DetectionEngine detectionEngine;

    public TransactionChangeListener(
            ObjectMapper objectMapper,
            TransactionRepository transactionRepository,
            DetectionEngine detectionEngine) {
        this.objectMapper = objectMapper;
        this.transactionRepository = transactionRepository;
        this.detectionEngine = detectionEngine;
    }

    /**
     * Deliberately has no try/catch: a failure rolls the transaction back and Spring Kafka's
     * error handler decides what happens to the record, rather than this method committing
     * half an evaluation. There is no dead-letter topic in this prototype.
     */
    @KafkaListener(topics = "${sentinel.engine.topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void onChange(String message) {
        Optional<Long> txnId = transactionIdOf(message);
        if (txnId.isEmpty()) {
            return;
        }
        Transaction transaction = transactionRepository.findById(txnId.get()).orElse(null);
        if (transaction == null) {
            log.warn("Change event for transaction id {} but no such row; skipping", txnId.get());
            return;
        }
        int alerts = detectionEngine.evaluate(transaction).size();
        log.info("Evaluated transaction {}; alerts={}", transaction.getTxnRef(), alerts);
    }

    /**
     * Pulls the primary key out of the change event, or empty when the event is not a new row.
     * Accepts the envelope with or without its schema, so the connector's
     * {@code value.converter.schemas.enable} setting does not matter here.
     */
    private Optional<Long> transactionIdOf(String message) {
        if (message == null || message.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode root = objectMapper.readTree(message);
            JsonNode envelope = root.has("payload") ? root.get("payload") : root;
            String operation = envelope.path("op").asText();
            if (!EVALUATED_OPS.contains(operation)) {
                log.debug("Ignoring change event with op={}", operation);
                return Optional.empty();
            }
            JsonNode id = envelope.path("after").path("id");
            if (!id.isNumber()) {
                log.warn("Change event has no numeric after.id; skipping");
                return Optional.empty();
            }
            return Optional.of(id.asLong());
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Unreadable change event; skipping", e);
            return Optional.empty();
        }
    }
}
