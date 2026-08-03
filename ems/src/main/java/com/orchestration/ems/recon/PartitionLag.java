package com.orchestration.ems.recon;

/**
 * One consumer-group partition's position, as two records-denominated distances (§10).
 *
 * <p>Both numbers are derived from the same three broker-side offsets and answer opposite questions:
 * <pre>
 *   earliest ............ committed ............ end
 *            &lt;- headroom -&gt;          &lt;-- lag --&gt;
 * </pre>
 * <ul>
 *   <li>{@code lag} = {@code end − committed} — how far behind the head we are. Rises when a partition
 *       is parked (§4.2 transient park) or simply outpaced.</li>
 *   <li>{@code retentionHeadroom} = {@code committed − earliest} — how far <em>ahead</em> of the log
 *       start we are, i.e. how much slack remains before retention deletes records the group has not
 *       consumed. §10 asks for "lag age approaching retention"; a true age needs an
 *       {@code offsetsForTimes} round-trip per partition per tick. Records-of-headroom is the cheap
 *       monotone proxy: it falls towards zero as the log start catches up with the committed offset,
 *       and it trips <b>before</b> anything is actually lost. This is a proxy for an underspecified
 *       §10 metric, not a contradiction of it (Phase-4 plan, micro-decision 6).</li>
 * </ul>
 *
 * <p>Both are clamped at zero — see {@link ConsumerLagProbe}.
 */
public record PartitionLag(String topic, int partition, long lag, long retentionHeadroom) { }
