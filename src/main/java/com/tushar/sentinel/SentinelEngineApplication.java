package com.tushar.sentinel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Detection runs here, not in the ingestion service. The engine consumes row-level change
 * events for the transaction table and evaluates the rule book against each new row.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class SentinelEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(SentinelEngineApplication.class, args);
    }
}
