package com.orchestration.ems.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;

import com.orchestration.ems.dispatch.AirflowTriggerClient.Outcome;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Unit proof of {@link OutboxDispatcher}'s mark/retain/backoff decisions with the repo and Airflow client
 * mocked (the real Postgres + WireMock drain is Batch I's {@code KillAirflowDrainIT}):
 * <ul>
 *   <li>a {@link Outcome#DELIVERED} row (200 <em>or</em> 409) is marked delivered, never retried;</li>
 *   <li>a {@link Outcome#RETRIABLE} row is retained with an incremented attempt <em>and</em> a future
 *       eligibility gate — a second immediate drain does <b>not</b> re-call Airflow;</li>
 *   <li>the {@code @ConditionalOnProperty} gate means no dispatcher bean (hence no drain / no calls)
 *       unless {@code ems.dispatch.enabled=true}.</li>
 * </ul>
 */
class OutboxDispatcherTest {

    private static final PendingTrigger TRIGGER =
            new PendingTrigger("a1b2c3d4e5f60718", "control_merival", "{\"eventId\":\"evt-1\"}", 0);

    private OutboxRepo outboxRepo;
    private AirflowTriggerClient triggerClient;
    private OutboxDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        outboxRepo = mock(OutboxRepo.class);
        triggerClient = mock(AirflowTriggerClient.class);
        when(outboxRepo.oldestPendingCreatedAt()).thenReturn(Optional.empty());
        dispatcher = new OutboxDispatcher(outboxRepo, triggerClient, new SimpleMeterRegistry(),
                noOpTransactionManager(), 100, 30, 600);
    }

    @Test
    void deliveredRow_isMarkedAndNotRetried() {
        when(outboxRepo.drainPending(anyInt())).thenReturn(List.of(TRIGGER));
        // DELIVERED is what the client returns for BOTH a 200 and a 409 (already-triggered, A6).
        when(triggerClient.trigger(TRIGGER.dagId(), TRIGGER.dagRunId(), TRIGGER.conf()))
                .thenReturn(Outcome.DELIVERED);

        dispatcher.drain();

        verify(outboxRepo).markDelivered(TRIGGER.dagRunId());
        verify(outboxRepo, never()).recordAttempt(eq(TRIGGER.dagRunId()), any());
    }

    @Test
    void retriableRow_isRetainedWithAttemptAndFutureEligibility() {
        when(outboxRepo.drainPending(anyInt())).thenReturn(List.of(TRIGGER));
        when(triggerClient.trigger(TRIGGER.dagId(), TRIGGER.dagRunId(), TRIGGER.conf()))
                .thenReturn(Outcome.RETRIABLE);

        dispatcher.drain(); // first tick: attempt fails, backoff armed (>= 15s)
        dispatcher.drain(); // second tick, immediate: row is within its backoff window → skipped

        // exactly one delivery attempt and one recorded failure despite two drains (future eligibility held)
        verify(triggerClient, times(1)).trigger(TRIGGER.dagId(), TRIGGER.dagRunId(), TRIGGER.conf());
        verify(outboxRepo, times(1)).recordAttempt(eq(TRIGGER.dagRunId()), any());
        verify(outboxRepo, never()).markDelivered(TRIGGER.dagRunId());
    }

    @Test
    void backoff_growsExponentiallyWithinCap_andRespectsJitterFloor() {
        // attempt 1: base 30s → [15, 30]; attempt 2: 60s → [30, 60]; deep attempt: capped 600s → [300, 600]
        assertThat(dispatcher.backoffFor(1)).isBetween(Duration.ofSeconds(15), Duration.ofSeconds(30));
        assertThat(dispatcher.backoffFor(2)).isBetween(Duration.ofSeconds(30), Duration.ofSeconds(60));
        assertThat(dispatcher.backoffFor(20)).isBetween(Duration.ofSeconds(300), Duration.ofSeconds(600));
    }

    @Test
    void dispatcherBean_absentUnlessEnabled() {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withUserConfiguration(DispatcherTestConfig.class);

        runner.run(context -> assertThat(context).doesNotHaveBean(OutboxDispatcher.class));
        runner.withPropertyValues("ems.dispatch.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(OutboxDispatcher.class));
    }

    /** A {@link PlatformTransactionManager} that runs the callback with no real transaction (returns null status). */
    private static PlatformTransactionManager noOpTransactionManager() {
        return mock(PlatformTransactionManager.class);
    }

    @Configuration(proxyBeanMethods = false)
    @Import(OutboxDispatcher.class) // brings in @ConditionalOnProperty + @Value defaults for evaluation
    static class DispatcherTestConfig {

        @Bean
        OutboxRepo outboxRepo() {
            return mock(OutboxRepo.class);
        }

        @Bean
        AirflowTriggerClient airflowTriggerClient() {
            return mock(AirflowTriggerClient.class);
        }

        @Bean
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        PlatformTransactionManager transactionManager() {
            return mock(PlatformTransactionManager.class);
        }
    }
}
