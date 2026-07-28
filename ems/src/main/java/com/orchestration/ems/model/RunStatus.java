package com.orchestration.ems.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Lifecycle summary of a run over the event store, returned by {@code GET /run/status} (ems-design §4.5).
 * Serves the framework's {@code SlaAwareHttpTrigger} per-wake probes and its categorized timeout diagnosis
 * ({@code NEVER_STARTED} / {@code STARTED_NO_TERMINAL} / {@code TERMINAL_IN_DLQ}) — <b>framework step F0
 * hard-depends on this endpoint</b> (ems-design §3, framework_redesign_final_implementation_plan.md F0).
 *
 * <p>Wire shape (byte-exact field names — verified against ems-design §4.5:217):
 * {@code {scheduled, started, terminal:{present, successful, event_id}, dlq_hint, last_event_at}}.
 *
 * <p>How each field is derived from the event store (all answerable from promoted columns + {@code dlq_record}):
 * <ul>
 *   <li>{@code started} — at least one event matches the correlation criteria (the run produced events).
 *       The framework discriminates {@code NEVER_STARTED} (no started) from {@code STARTED_NO_TERMINAL}
 *       (started, no terminal) on exactly this flag (framework §6.2 rule 3).</li>
 *   <li>{@code scheduled} — EMS has <b>no scheduler view</b> distinct from the event stream, so it collapses
 *       into {@code started}: any event evidences the run reached the system (micro-decision 5 — "derived
 *       from the event store unless an event evidences it"). A truer {@code scheduled} is a §14 follow-up if
 *       the framework ever needs to distinguish scheduled-but-not-started.</li>
 *   <li>{@code terminal} — see {@link Terminal}. Terminal vocabulary is the framework's two completion
 *       schemes (framework §333): {@code STATE ∈ {FINISH, FAILED}} (MERIVAL_CALC_EVENT) or
 *       {@code taskEventType = COMPLETED} (MEG_TASK_EVENT).</li>
 *   <li>{@code dlqHint} — a {@code dlq_record} row matches the same correlation keys
 *       ({@code ix_dlq_context}/{@code ix_dlq_task}); the framework maps a deadline-passed run with this hint
 *       set to {@code TERMINAL_IN_DLQ}.</li>
 *   <li>{@code lastEventAt} — ISO-8601 instant of the most recent matching event ({@code max(created_at)}),
 *       or {@code null} when nothing matched.</li>
 * </ul>
 *
 * @param scheduled   whether the run is known to the system (collapses into {@code started} — see above)
 * @param started     whether any event matches the correlation criteria
 * @param terminal     the terminal-event summary (never {@code null}; {@code present=false} when none)
 * @param dlqHint     whether a {@code dlq_record} matches the correlation keys
 * @param lastEventAt ISO-8601 instant of the newest matching event, or {@code null}
 */
public record RunStatus(
        boolean scheduled,
        boolean started,
        Terminal terminal,
        @JsonProperty("dlq_hint") boolean dlqHint,
        @JsonProperty("last_event_at") String lastEventAt) {

    /**
     * The terminal-event facts. When no terminal event is present, {@code present=false},
     * {@code successful=false}, {@code eventId=null}.
     *
     * @param present    whether a terminal event exists for the criteria
     * @param successful whether that terminal event signals success ({@code STATE=FINISH}, or a
     *                   {@code COMPLETED} task event whose {@code successful} flag is true)
     * @param eventId    the terminal event's id, or {@code null}
     */
    public record Terminal(
            boolean present,
            boolean successful,
            @JsonProperty("event_id") String eventId) {

        /** The "no terminal event yet" value — {@code {present:false, successful:false, event_id:null}}. */
        public static Terminal absent() {
            return new Terminal(false, false, null);
        }
    }
}
