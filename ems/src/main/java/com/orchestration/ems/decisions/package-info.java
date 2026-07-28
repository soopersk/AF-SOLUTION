/**
 * Routing-decision audit: {@code RoutingDecisionRepo} (L0 verdicts written in the ingest TX
 * with {@code ON CONFLICT DO NOTHING} against {@code ux_rd_l0}) and
 * {@code DecisionIngestController} ({@code POST /decisions}, by which the Phase-B+ dispatcher
 * records L1 summaries/outcomes and gate evaluations). See ems-design.md §5, trigger-plan §4.5.
 */
package com.orchestration.ems.decisions;
