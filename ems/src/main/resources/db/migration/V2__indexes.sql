-- ============================================================================
-- V2 — indexes  (ems-design §6, with Amendment A9)
-- Driven strictly by the observed access patterns; nothing speculative.
-- ============================================================================

-- event: taskId is THE calc-event lookup key; context_id serves the join + /run/status;
-- dataset_id secondary; created_at serves retention + ORDER BY + /gate/groups lookback.
CREATE INDEX idx_event_task_id    ON event (task_id);
CREATE INDEX idx_event_dataset_id ON event (dataset_id);
CREATE INDEX idx_event_context_id ON event (context_id);
CREATE INDEX idx_event_created_at ON event (created_at);

-- context: reporting_date + frequency are almost always paired; h3_region rides as the
-- 3rd column. Leftmost-prefix serves pair-only queries — one index, not three.
CREATE INDEX idx_context_rep_freq_region ON context (reporting_date, frequency, h3_region);
CREATE INDEX idx_context_dataset_id      ON context (dataset_id);
CREATE INDEX idx_context_created_at      ON context (created_at);

-- A9: parentIds is queried by array-containment (old-ems/DatabaseEventRepository.scala:33
--     `c.json->'parentIds' ?? ?`). GIN with jsonb_path_ops serves `parentIds @> to_jsonb(:id)`
--     for the STATUS CHECK driver and /parentcontext chain traversal — multi-parent safe.
CREATE INDEX idx_context_parent_ids ON context USING gin ((json->'parentIds') jsonb_path_ops);

-- Low-frequency / always-companioned params (source, type, state, taskEventType) get NO
-- dedicated indexes: in every observed query they co-occur with a selective key, so the
-- driving index narrows to a handful of rows and the residual filter is free. Add later
-- only if pg_stat_statements proves a need.
