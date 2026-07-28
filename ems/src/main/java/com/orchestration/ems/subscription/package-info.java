/**
 * Level-0 subscription evaluation: {@code SubscriptionService} (cel-java, compiled programs
 * cached per rule) and {@code SubscriptionRepo}. Enforces the two-stage contract (A4):
 * PERSIST rules reference {@code event.*} only (write-rejected otherwise); FORWARD rules may
 * reference {@code event.*} and {@code context.*}. Seed invariant: PERSIST &supe; FORWARD
 * (old-ems effective persist = PERSIST &cup; FORWARD), CI-checked. See ems-design.md §7.
 */
package com.orchestration.ems.subscription;
