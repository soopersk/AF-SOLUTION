package com.orchestration.ems.model;

import java.util.List;

/**
 * Wire model for {@code GET /gate/groups} (ems-design §4.5). The generic grouped-event answer the heartbeat
 * DAG consumes in one round trip (trigger_redesign_final_implementation_plan.md:355, §11 scenario): the
 * distinct {@code group_by} values with ≥ 1 event matching the criteria inside the lookback window, each with
 * its present-contributor set.
 *
 * <p>EMS stays <b>rule-free</b>: it knows nothing of the gate's <i>expected</i> contributor set or its
 * staleness policy — the caller (which holds the registry gate spec) diffs {@code contributors} against its
 * expected set and applies staleness itself. So this body is exactly the design sentence: {@code group} +
 * {@code contributors}, nothing more.
 *
 * <p><b>Deliberately omitted — flagged (trigger §11 tension):</b> the §11 heartbeat scenario renders a group's
 * <i>age</i> ("age &gt; staleness_window") alongside its present count, which hints at a per-group timestamp.
 * The authoritative EMS contract (ems-design §4.5) lists only "present-contributor set", and neither doc pins
 * a field name/format for the timestamp. Rather than invent one (a guess I'd have to break later), age is left
 * out here — the caller can obtain it via {@code GET /event}, and a per-group {@code last_event_at} is a
 * trivially additive field once the heartbeat contract names it. Same discipline as Batch C's {@code scheduled}.
 *
 * @param groups the distinct qualifying groups (deterministically ordered by {@code group} value)
 */
public record GateGroups(List<Group> groups) {

    /**
     * One qualifying group.
     *
     * @param group        the {@code group_by} json-path value shared by the group's events
     * @param contributors the distinct, non-null {@code contributor} json-path values present in the group
     *                     (sorted; empty when no {@code contributor} path was supplied or none resolved)
     */
    public record Group(String group, List<String> contributors) {
    }
}
