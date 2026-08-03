
INSERT INTO system_properties (property_key, property_value) VALUES
( 'airFlow.rest.authentication.user', '${AIRFLOW_USER}'),
( 'airFlow.rest.authentication.password', '${AIRFLOW_PASSWORD}'),
( 'azure.oauth2.client-id', '${AZURE_CLIENT_ID}'),
( 'azure.oauth2.client-secret', '${AZURE_CLIENT_SECRET}'),
( 'trust-store-location', 'file:///etc/secrets/megdp-kafka.jks'),
( 'trust-store-password', '${JKS_PASSWORD}'),
( 'edf.oasys.consumer.api.key', '${EDF_OASYS.CONSUMER_API_KEY}'),
( 'edf.oasys.consumer.api.secret', '${EDF_OASYS.CONSUMER_API_SECRET}'),
( 'edf.oasys.registry.api.key', '${EDF_OASYS_SCHEMA_REGISTRY_API_KEY}'),
( 'edf.oasys.registry.api.secret', '${EDF_OASYS_SCHEMA_REGISTRY_API_SECRET}'),
( 'spring.kafka.consumer.auto-offset-reset', '${auto_offset_reset:latest}'),
( 'spring.kafka.consumer.max-poll-records', '${max_poll_recording:500}'),
( 'spring.kafka.consumer.isolation-level', '${isolation_level:read_committed}'),
( 'spring.kafka.consumer.fetch-min-size', '${fetch_min_size:64000}'),
( 'spring.kafka.consumer.enable-auto-commit', '${enable_auto_commit:true}'),
( 'spring.kafka.consumer.properties.partition.assignment.strategy', '${partition_assignment_strategy}'),
( 'spring.kafka.consumer.properties.auth-exception-retry-interval', '${auth_exception_retry_interval}'),
( 'management.endpoint.health.probes.enabled', '${LIVENESS_PROBE_ENABLED}'),
( 'management.endpoints.web.exposure.include', 'health,livenessstate')
ON CONFLICT (property_key) DO NOTHING;


INSERT INTO system_properties (property_key, property_value) VALUES
( 'eventorchestration.filter.persist',
'[' ||
'{"$.additionalData.tenant":"FRCA"}, ' ||
'{"$.additionalData.tenant":"AQUA_CCR", "$.additionalData.batchtype":"INTRA-MONTH-ADJUSTED"}, ' ||
'{"$.additionalData.tenant":"AQUA_CCR", "$.additionalData.batchtype":"INTRA-MONTH-UNADJUSTED"}, ' ||
'{"$.source":"MERIVAL", "$.additionalData.TYPE":"INGESTION", "$.additionalData.RUN_TYPE":"BATCH"}, ' ||
'{"$.source":"MERIVAL", "$.additionalData.TYPE":"INGESTION", "$.additionalData.RUN_TYPE":"INTRA"}, ' ||
'{"$.source":"RWA", "$.additionalData.tenant":"MR", "$.additionalData.FREQUENCY":"MONTHLY"}, ' ||
'{"$.source":"CVA", "$.additionalData.tenant":"MR", "$.additionalData.FREQUENCY":"MONTHLY"}' ||
']' )
ON CONFLICT (property_key) DO UPDATE SET property_value = excluded.property_value;

COMMIT;

INSERT INTO post_filter_control_dag_map (team, filter_name, filter_condition, dag_id, enabled) VALUES
( 'CAPITAL', 'cap_data_update.FRCA_CURATION', '$.additionalData.tenant:FRCA.msgTypeEventType:data-update, $.additionalData.tenant:FRCA.updateType:CURATION,$.context.date.run-category:TOPSIDE.*'
, 'orchestration_control_dag_capital', TRUE),
( 'CAPITAL', 'cap_data_update.FRCA_CALC',  '$.additionalData.tenant:FRCA.updateType:CALC_EVENT,$.additionalData.STATE:FINISH', 'orchestration_control_dag_capital', TRUE),
( 'CAPITAL', 'cap_AQUA_CCR_ADJUSTED', '$.additionalData.tenant:AQUA_CCR,$.additionalData.batchType:INTRA-MONTH-ADJUSTED', 'orchestration_control_dag_capital',      TRUE),
( 'CAPITAL', 'cap_AQUA_CCR_UNADJUSTED', '$.additionalData.tenant:AQUA_CCR,$.additionalData.batchType:INTRA-MONTH-UNADJUSTED', 'orchestration_control_dag_capital',  TRUE),
( 'CAPITAL', 'cap_data_update.MER_batch', '$.source:MERYVAL,$.additionalData.TYPE:INGESTION,$.additionalData.RUN_TYPE:BATCH', 'orchestration_control_dag_capital',  TRUE),
( 'CAPITAL', 'cap_data_update.MER_intra', '$.source:MERYVAL,$.additionalData.TYPE:INGESTION,$.additionalData.RUN_TYPE:INTRA', 'orchestration_control_dag_capital',  TRUE),
( 'CAPITAL', 'cap_RWA', '$.source:RWA,$.additionalData.tenant:MR,$.additionalData.FREQUENCY:MONTHLY,$.additionalData.STATE:FINISH', 'orchestration_control_dag_capital', TRUE),
( 'CAPITAL', 'cap_CVA', '$.source:CVA,$.additionalData.tenant:MR,$.additionalData.FREQUENCY:MONTHLY,$.additionalData.STATE:FINISH', 'orchestration_control_dag_capital', TRUE),
( 'NSFR', 'nsfr_data-update', '$.additionalData.msgTypeEventType:data-update,$.additionalData.tenant:ACTL,$.additionalData.updateType:CURATION', 'orchestration_control_dag_liquidity', FALSE)
ON CONFLICT (team, filter_name) DO UPDATE SET filter_condition = EXCLUDED.filter_condition, dag_id = EXCLUDED.dag_id, enabled = EXCLUDED.enabled;