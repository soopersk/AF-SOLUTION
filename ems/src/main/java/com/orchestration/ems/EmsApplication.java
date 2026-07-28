package com.orchestration.ems;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Event Management Service — trigger-plan Phase A.
 *
 * <p>A thin, rule-free ingest + query service (the "dumb pipe" of the trigger redesign):
 * consume events from Kafka, evaluate Level-0 subscriptions (CEL), enrich from context,
 * persist event/context/decision/outbox in one transaction, and serve fast reads to the
 * Airflow-resident control plane. All routing intelligence lives in the control plane;
 * EMS owns durability and idempotency.
 *
 * <p>{@code @EnableScheduling} drives the outbox dispatcher and the reconciliation sweep
 * (ems-design §4.4 / §10).
 */
@SpringBootApplication
@EnableScheduling
public class EmsApplication {

    public static void main(String[] args) {
        SpringApplication.run(EmsApplication.class, args);
    }
}
