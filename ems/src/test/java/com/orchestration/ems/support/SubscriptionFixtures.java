package com.orchestration.ems.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;

/**
 * Loads the seed-0 subscription TEST fixtures ({@code /fixtures/subscriptions_seed0.json}) from the
 * classpath. These are the mechanical CEL translation of the legacy inventory
 * ({@code old-ems/properties.sql} lines 25–52); see {@code subscriptions_seed0.provenance.md}.
 *
 * <p>Rows are returned as the production {@code model/SubscriptionRow} with {@code id = 0L} (the
 * fixture JSON carries no DB IDENTITY). The private {@link Seed0Dto} mirrors the JSON fields 1:1
 * (which match the V3 {@code subscription} columns) and is mapped onto {@code SubscriptionRow}.
 */
public final class SubscriptionFixtures {

    /** Classpath location of the seed-0 fixture. */
    public static final String RESOURCE = "/fixtures/subscriptions_seed0.json";

    // Strict by design: an unknown/mistyped fixture key (e.g. "controDagId") must fail loudly
    // rather than silently null the field.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SubscriptionFixtures() {
    }

    /**
     * A minimal JSON carrier for one seed-0 subscription row. Field names match the fixture JSON
     * and the V3 {@code subscription} table columns; mapped onto {@code SubscriptionRow} by
     * {@link #seed0()}.
     */
    private record Seed0Dto(
            String tenantId,
            String stage,
            String ruleName,
            String controlDagId,
            String whenCel,
            String registryVersion,
            boolean enabled) {
    }

    /** Loads all 16 seed-0 rows (7 PERSIST + 8 CAPITAL FORWARD + 1 disabled NSFR FORWARD). */
    public static List<SubscriptionRow> seed0() {
        try (InputStream in = SubscriptionFixtures.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing test fixture on classpath: " + RESOURCE);
            }
            List<Seed0Dto> dtos = MAPPER.readValue(in, new TypeReference<List<Seed0Dto>>() {
            });
            return dtos.stream()
                    .map(d -> new SubscriptionRow(
                            0L,
                            d.tenantId(),
                            Stage.valueOf(d.stage()),
                            d.ruleName(),
                            d.controlDagId(),
                            d.whenCel(),
                            d.registryVersion(),
                            d.enabled()))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load fixture " + RESOURCE, e);
        }
    }
}
