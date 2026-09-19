package com.tushar.sentinel.repository.alert;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AlertRepository extends JpaRepository<Alert, Long> {

    /** Natural key of the pattern, so re-evaluating a transaction updates its alert, never duplicates it. */
    Optional<Alert> findByDedupKey(String dedupKey);
}
