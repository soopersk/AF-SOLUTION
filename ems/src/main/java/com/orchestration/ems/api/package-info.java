/**
 * Query + admin HTTP surface: {@code EventController} ({@code /event}, byte-compatible
 * enriched JSON, exact 200/404 semantics), {@code ContextController}
 * ({@code /context}, {@code /parentcontext}, {@code /childcontext}),
 * {@code RunStatusController} ({@code /run/status} — the framework F0 unblock, incl.
 * {@code dlq_hint}), {@code GateGroupsController} ({@code /gate/groups} generic grouping),
 * {@code AdminController} ({@code POST /admin/replay}, {@code PUT /admin/subscriptions}),
 * and {@code TokenController}. See ems-design.md §8, trigger-plan §7.
 */
package com.orchestration.ems.api;
